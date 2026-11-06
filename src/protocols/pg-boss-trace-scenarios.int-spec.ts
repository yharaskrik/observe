import {
  Controller,
  Inject,
  Injectable,
  Logger,
  Module,
  Param,
  Post,
} from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { NestExpressApplication } from "@nestjs/platform-express";
import { connect } from "net";
import { Pool } from "pg";
import { PgBoss } from "pg-boss";
import request from "supertest";
import { JobSnapshot } from "../interfaces/index.js";
import { createObserveModule } from "../observe.module.js";
import {
  CollectedJobSnapshots,
  CollectedSnapshots,
  collectJobSnapshots,
  collectSnapshots,
  testObserveOptions,
  waitForJobSnapshot,
  waitForSnapshot,
} from "../testing/observe-harness.js";

const POSTGRES = {
  host: process.env.PGHOST ?? "127.0.0.1",
  port: Number(process.env.PGPORT ?? 54321),
  user: process.env.PGUSER ?? "postgres",
  password: process.env.PGPASSWORD ?? "postgres-whisprr",
  database: process.env.PGDATABASE ?? "postgres",
};

/** pg-boss has no in-memory driver, so this suite runs only where Postgres answers. */
const postgresReachable = await new Promise<boolean>((resolve) => {
  const socket = connect({ host: POSTGRES.host, port: POSTGRES.port });
  const finish = (reachable: boolean) => {
    socket.destroy();
    resolve(reachable);
  };
  socket.setTimeout(500, () => finish(false));
  socket.once("connect", () => finish(true));
  socket.once("error", () => finish(false));
});

const SCHEMA = `observe_int_pgboss_${process.pid}`;
const BOSS = Symbol("BOSS");
const UNTRACED_BOSS = Symbol("UNTRACED_BOSS");
const UUID = /^[0-9a-f-]{36}$/;

/** Long enough for a poll, a retry and a delayed job to come round. */
const JOB_TIMEOUT_MS = 15_000;
const WORK = { pollingIntervalSeconds: 0.5 };

const Q = {
  signup: "signup-mail",
  insert: "insert-mail",
  later: "later-mail",
  parent: "flow-parent",
  child: "flow-child",
  publishA: "publish-a",
  publishB: "publish-b",
  chainFrom: "chain-from",
  chainTo: "chain-to",
  retry: "retry-mail",
  batch: "batch-mail",
  transactional: "tx-mail",
  registeredInRequest: "registered-in-request",
  orphan: "orphan-mail",
  untraced: "untraced-mail",
} as const;

const { ObserveModule, ObserveInstrument } = createObserveModule();

@Injectable()
class MailerService {
  deliver() {
    return "delivered";
  }
}

/** Resolves once a handler has run, so a request can stay open across its job. */
const handled = new Map<string, () => void>();
const whenHandled = (key: string) =>
  new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`${key} was never handled`)),
      JOB_TIMEOUT_MS,
    );
    handled.set(key, () => {
      clearTimeout(timer);
      resolve();
    });
  });

@Controller()
class EnqueueController {
  constructor(
    @Inject(BOSS) private readonly boss: PgBoss,
    @Inject(UNTRACED_BOSS) private readonly untracedBoss: PgBoss,
    private readonly mailer: MailerService,
  ) {}

  @Post("signup")
  async signup() {
    const done = whenHandled(Q.signup);
    await this.boss.send(Q.signup, {});
    // Still open while the worker - in this same process - runs the job under
    // this request's id, so both snapshots have to survive sharing it.
    await done;
    return { ok: true };
  }

  @Post("insert")
  async insert() {
    await this.boss.insert(Q.insert, [{ data: {} }]);
    return { ok: true };
  }

  @Post("later")
  async later() {
    await this.boss.sendAfter(Q.later, {}, null, 1);
    return { ok: true };
  }

  @Post("flow")
  async flow() {
    await this.boss.flow([
      { ref: "parent", name: Q.parent },
      { ref: "child", name: Q.child, dependsOn: ["parent"] },
    ]);
    return { ok: true };
  }

  @Post("publish")
  async publish() {
    await this.boss.publish("observe-int-event", {});
    return { ok: true };
  }

  @Post("chain")
  async chain() {
    await this.boss.send(Q.chainFrom, {});
    return { ok: true };
  }

  @Post("send/:queue")
  async send(@Param("queue") queue: string) {
    return { id: await this.boss.send(queue, {}) };
  }

  @Post("batch")
  async batch() {
    await this.boss.send(Q.batch, {});
    await this.boss.send(Q.batch, {});
    return { ok: true };
  }

  @Post("register-worker")
  async registerWorker() {
    await this.boss.work(Q.registeredInRequest, WORK, async () =>
      this.mailer.deliver(),
    );
    return { ok: true };
  }

  @Post("untraced")
  async untraced() {
    await this.untracedBoss.send(Q.untraced, {});
    return { ok: true };
  }
}

@Module({
  imports: [ObserveModule.forRoot(testObserveOptions())],
  controllers: [EnqueueController],
  providers: [
    MailerService,
    {
      provide: BOSS,
      useFactory: () => new PgBoss({ ...POSTGRES, schema: SCHEMA }),
    },
    {
      provide: UNTRACED_BOSS,
      useFactory: () =>
        new PgBoss({
          ...POSTGRES,
          schema: SCHEMA,
          openTelemetry: { enabled: false },
          schedule: false,
          supervise: false,
          migrate: false,
        }),
    },
  ],
})
class PgBossTestModule {}

/**
 * A pg-boss job reports under the trace of the operation that enqueued it.
 *
 * The id travels in the job row's `trace_context` column, written through
 * pg-boss's telemetry on every way of enqueueing and read back by its fetch
 * SQL, so only a real Postgres proves the round trip - and that the agent
 * patched the very `Telemetry` module pg-boss itself runs on.
 */
describe.skipIf(!postgresReachable)(
  "ObserveModule: pg-boss trace scenarios",
  () => {
    let app: NestExpressApplication;
    let requests: CollectedSnapshots;
    let jobs: CollectedJobSnapshots;
    let boss: PgBoss;
    let untracedBoss: PgBoss;
    let pool: Pool;
    let mailer: MailerService;
    const warn = vi.spyOn(Logger.prototype, "warn");

    const job = (queue: string, timeoutMs = JOB_TIMEOUT_MS) =>
      waitForJobSnapshot(jobs, (item) => item.queueName === queue, timeoutMs);

    const jobsOf = (queue: string) =>
      jobs.items.filter((item) => item.queueName === queue);

    const post = async (path: string, traceId: string) =>
      (
        await request(app.getHttpServer())
          .post(path)
          .set("x-request-id", traceId)
          .expect(201)
      ).body as Record<string, unknown>;

    beforeAll(async () => {
      pool = new Pool(POSTGRES);
      await pool.query(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`);

      app = await NestFactory.create<NestExpressApplication>(PgBossTestModule, {
        instrument: ObserveInstrument,
        logger: false,
      });
      requests = collectSnapshots(app);
      jobs = collectJobSnapshots(app);
      await app.init();

      boss = app.get<PgBoss>(BOSS);
      untracedBoss = app.get<PgBoss>(UNTRACED_BOSS);
      boss.on("error", () => undefined);
      untracedBoss.on("error", () => undefined);
      await boss.start();
      await untracedBoss.start();

      for (const queue of Object.values(Q)) {
        await boss.createQueue(
          queue,
          queue === Q.retry ? { retryLimit: 1, retryDelay: 0 } : {},
        );
      }
      await boss.subscribe("observe-int-event", Q.publishA);
      await boss.subscribe("observe-int-event", Q.publishB);

      mailer = app.get(MailerService);
      const simple = [
        Q.insert,
        Q.later,
        Q.parent,
        Q.child,
        Q.publishA,
        Q.publishB,
        Q.chainTo,
        Q.orphan,
      ];
      for (const queue of simple) {
        await boss.work(queue, WORK, async () => mailer.deliver());
      }
      await boss.work(Q.signup, WORK, async () => {
        mailer.deliver();
        handled.get(Q.signup)?.();
      });
      await boss.work(Q.chainFrom, WORK, async () => {
        await boss.send(Q.chainTo, {});
      });
      let retryAttempts = 0;
      await boss.work(Q.retry, WORK, async () => {
        retryAttempts += 1;
        mailer.deliver();
        if (retryAttempts === 1) {
          throw new Error("first attempt fails");
        }
      });
      await untracedBoss.work(Q.untraced, WORK, async () => mailer.deliver());
    });

    afterAll(async () => {
      await boss?.stop({ graceful: false });
      await untracedBoss?.stop({ graceful: false });
      await app?.close();
      await pool?.query(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`);
      await pool?.end();
    });

    it("runs a job under the enqueuing request's trace id, and keeps both snapshots", async () => {
      await post("/signup", "pgboss-signup-1");

      const run = await job(Q.signup);
      expect(run).toMatchObject({
        traceId: "pgboss-signup-1",
        status: "completed",
        name: Q.signup,
        id: expect.stringMatching(UUID),
        attemptsMade: 0,
      });
      expect(run.traces[0]).toMatchObject({
        className: "MailerService",
        methodKey: "deliver",
      });

      const http = await waitForSnapshot(
        requests,
        (item) => item.operationId === "/signup",
      );
      expect(http.traceId).toBe("pgboss-signup-1");
    });

    it("persists the trace id in the job's trace_context column", async () => {
      const { id } = await post(`/send/${Q.orphan}`, "pgboss-column-1");

      const { rows } = await pool.query<{ trace_context: unknown }>(
        `SELECT trace_context FROM ${SCHEMA}.job WHERE id = $1`,
        [id],
      );
      expect(rows[0]?.trace_context).toMatchObject({
        observeTraceId: "pgboss-column-1",
      });
    });

    it.each([
      ["insert", "/insert", Q.insert],
      ["sendAfter", "/later", Q.later],
    ])("inherits through %s", async (_label, path, queue) => {
      await post(path, `pgboss-${queue}`);

      expect((await job(queue)).traceId).toBe(`pgboss-${queue}`);
    });

    it("inherits through flow, parent and dependent alike", async () => {
      await post("/flow", "pgboss-flow-1");

      expect((await job(Q.parent)).traceId).toBe("pgboss-flow-1");
      expect((await job(Q.child)).traceId).toBe("pgboss-flow-1");
    });

    it("inherits through publish, on every subscribed queue", async () => {
      await post("/publish", "pgboss-publish-1");

      expect((await job(Q.publishA)).traceId).toBe("pgboss-publish-1");
      expect((await job(Q.publishB)).traceId).toBe("pgboss-publish-1");
    });

    it("carries the trace on to a job enqueued from inside a job", async () => {
      await post("/chain", "pgboss-chain-1");

      expect((await job(Q.chainFrom)).traceId).toBe("pgboss-chain-1");
      expect((await job(Q.chainTo)).traceId).toBe("pgboss-chain-1");
    });

    it("keeps the trace id across a retry", async () => {
      await post(`/send/${Q.retry}`, "pgboss-retry-1");

      await waitForJobSnapshot(
        jobs,
        (item) => item.queueName === Q.retry && item.status === "completed",
        JOB_TIMEOUT_MS,
      );
      const runs = jobsOf(Q.retry);
      expect(
        runs.map(({ traceId, status, attemptsMade }) => ({
          traceId,
          status,
          attemptsMade,
        })),
      ).toEqual([
        { traceId: "pgboss-retry-1", status: "failed", attemptsMade: 0 },
        { traceId: "pgboss-retry-1", status: "completed", attemptsMade: 1 },
      ]);
    });

    it("gives a batch one trace of its own", async () => {
      await post("/batch", "pgboss-batch-1");
      await boss.work(Q.batch, { ...WORK, batchSize: 2 }, async () =>
        mailer.deliver(),
      );

      const run = await job(Q.batch);
      expect(run.id).toBeUndefined();
      expect(run.traceId).toMatch(UUID);
      expect(jobsOf(Q.batch)).toHaveLength(1);
    });

    it("hands a transactional worker its db, under the enqueuing trace", async () => {
      let db: unknown;
      await boss.work(
        Q.transactional,
        { ...WORK, transactional: true },
        async (_jobs, tx) => {
          db = tx;
          mailer.deliver();
        },
      );
      await post(`/send/${Q.transactional}`, "pgboss-tx-1");

      expect((await job(Q.transactional)).traceId).toBe("pgboss-tx-1");
      expect(db).toMatchObject({ executeSql: expect.any(Function) });
    });

    it("does not fold jobs into the request that registered their worker", async () => {
      await post("/register-worker", "pgboss-registering-1");
      await boss.send(Q.registeredInRequest, {});

      const run = await job(Q.registeredInRequest);
      expect(run.traceId).toMatch(UUID);
      const http = await waitForSnapshot(
        requests,
        (item) => item.operationId === "/register-worker",
      );
      // The worker's polling would otherwise have joined this request.
      expect(http.traceId).toBe("pgboss-registering-1");
    });

    it("mints a fresh id for a job enqueued outside any trace", async () => {
      await boss.send(Q.orphan, { orphan: true });

      const run = await waitForJobSnapshot(
        jobs,
        (item) =>
          item.queueName === Q.orphan && item.traceId !== "pgboss-column-1",
        JOB_TIMEOUT_MS,
      );
      expect(run.traceId).toMatch(UUID);
    });

    it("mints its own id, and says why, when pg-boss telemetry is off", async () => {
      await post("/untraced", "pgboss-untraced-1");

      const run: JobSnapshot = await job(Q.untraced);
      expect(run.traceId).toMatch(UUID);
      expect(
        warn.mock.calls.some(([message]) =>
          String(message).includes("openTelemetry.enabled false"),
        ),
      ).toBe(true);
    });
  },
);
