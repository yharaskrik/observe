import { Logger } from "@nestjs/common";
import { AsyncLocalStorage } from "async_hooks";
import { PgBoss } from "pg-boss";
import Telemetry, { type TraceCarrier } from "pg-boss/dist/telemetry.js";
import { OperationTraceRegistry } from "../services/operation-trace.registry.js";
import { PgBossObserveAgentService } from "./pg-boss-observe-agent.service.js";

type Handler = (jobs: unknown[]) => unknown;

const ORIGINAL_WORK = Symbol.for("nestjs.observe.pgBoss.work");
const prototype = PgBoss.prototype as unknown as Record<
  string | symbol,
  unknown
>;
const realWork = prototype.work;

const telemetryPrototype = Telemetry.prototype as unknown as Record<
  string | symbol,
  unknown
>;
const realSend = telemetryPrototype.send;
const realProcess = telemetryPrototype.process;
const ORIGINAL_SEND = Symbol.for("nestjs.observe.pgBoss.telemetry.send");
const ORIGINAL_PROCESS = Symbol.for("nestjs.observe.pgBoss.telemetry.process");

/** pg-boss's own telemetry, as a PgBoss client builds it - no database needed. */
const telemetry = (enabled = true) =>
  new Telemetry({ enabled }, "pgboss", () => null);

type Carrier = TraceCarrier | null;

/**
 * Stands in for pg-boss's own `work`, which needs a started client and a
 * database. The agent wraps whatever is parked under `ORIGINAL_WORK`, so the
 * stub receives the handler exactly as pg-boss would.
 */
const stubOriginalWork = () => {
  const original = vi.fn(
    (_name: string, ..._args: unknown[]): unknown => "worker-id",
  );
  prototype[ORIGINAL_WORK] = original;
  return original;
};

const handlerPassedTo = (original: ReturnType<typeof stubOriginalWork>) => {
  const args = original.mock.calls[0] ?? [];
  return args[args.length - 1] as Handler;
};

const createAgent = (options: Record<string, unknown> = {}) => {
  const als = new AsyncLocalStorage<Map<string, any>>();
  const registry = new OperationTraceRegistry(als as never, false);
  const startTrace = vi.spyOn(registry, "startTrace");
  const endTrace = vi.spyOn(registry, "endTrace");
  const agent = new PgBossObserveAgentService(
    { insertJobSnapshot: vi.fn(), isReporting: () => true } as never,
    { traceIdKey: "traceId", ...options } as never,
    registry,
    als,
  );
  /** What the run reported: the snapshot it opened, and how it ended. */
  const reported = () => ({
    snapshot: startTrace.mock.calls[0]?.[1] as Record<string, unknown>,
    status: (endTrace.mock.calls[0]?.[1] as { status?: string } | undefined)
      ?.status,
  });
  return { agent, startTrace, endTrace, reported, als };
};

const work = (...args: unknown[]) =>
  (prototype.work as (...args: unknown[]) => unknown).apply(
    Object.create(PgBoss.prototype),
    args,
  );

afterEach(() => {
  prototype.work = realWork;
  delete prototype[ORIGINAL_WORK];
  telemetryPrototype.send = realSend;
  telemetryPrototype.process = realProcess;
  delete telemetryPrototype[ORIGINAL_SEND];
  delete telemetryPrototype[ORIGINAL_PROCESS];
  vi.restoreAllMocks();
});

describe("PgBossObserveAgentService", () => {
  it("stays inert when pg-boss is not installed", () => {
    vi.spyOn(
      PgBossObserveAgentService.prototype as unknown as {
        loadPgBossPrototype: () => unknown;
      },
      "loadPgBossPrototype",
    ).mockReturnValue(undefined);

    expect(() => createAgent()).not.toThrow();
    expect(prototype.work).toBe(realWork);
  });

  it("passes the queue name, options and worker id through", () => {
    const original = stubOriginalWork();
    createAgent();
    const options = { batchSize: 1, includeMetadata: true };

    const result = work("emails", options, async () => undefined);

    expect(result).toBe("worker-id");
    expect(original).toHaveBeenCalledWith(
      "emails",
      options,
      expect.any(Function),
    );
  });

  it("reports a single job with its id, wait time and attempts", async () => {
    const original = stubOriginalWork();
    const { endTrace, reported } = createAgent();
    const handler = vi.fn(async () => "done");
    work("emails", handler);

    const job = {
      id: "job-1",
      name: "emails",
      retryCount: 1,
      retryLimit: 3,
      createdOn: new Date("2026-01-01T00:00:00.000Z"),
      startAfter: new Date("2026-01-01T00:00:02.000Z"),
      startedOn: new Date("2026-01-01T00:00:05.000Z"),
    };
    await expect(handlerPassedTo(original)([job])).resolves.toBe("done");

    expect(handler).toHaveBeenCalledWith([job]);
    await vi.waitFor(() => expect(endTrace).toHaveBeenCalledOnce());
    expect(reported().status).toBe("completed");
    expect(reported().snapshot).toMatchObject({
      queueName: "emails",
      name: "emails",
      id: "job-1",
      enqueuedAt: "2026-01-01T00:00:00.000Z",
      waitDuration: 3000,
      attemptsMade: 1,
      maxAttempts: 4,
    });
  });

  it("reports a batch as one run of the queue, without a job id", async () => {
    const original = stubOriginalWork();
    const { startTrace, reported } = createAgent();
    work("emails", { batchSize: 5 }, async () => undefined);

    await handlerPassedTo(original)([
      { id: "a", name: "emails" },
      { id: "b", name: "emails" },
    ]);

    expect(startTrace).toHaveBeenCalledOnce();
    const { snapshot } = reported();
    expect(snapshot).toMatchObject({ queueName: "emails", name: "emails" });
    expect(snapshot.id).toBeUndefined();
  });

  it("reports a handler that throws as failed and rethrows", async () => {
    const original = stubOriginalWork();
    const { endTrace, reported } = createAgent();
    const error = new Error("boom");
    work("emails", async () => {
      throw error;
    });

    await expect(
      handlerPassedTo(original)([{ id: "job-1", name: "emails" }]),
    ).rejects.toBe(error);

    await vi.waitFor(() => expect(endTrace).toHaveBeenCalledOnce());
    expect(reported().status).toBe("failed");
  });

  it("runs the handler inside a trace of its own", async () => {
    const original = stubOriginalWork();
    const { als } = createAgent();
    let traceId: unknown;
    work("emails", async () => {
      traceId = als.getStore()?.get("traceId");
    });

    await handlerPassedTo(original)([{ id: "job-1", name: "emails" }]);

    expect(traceId).toEqual(expect.any(String));
  });

  it("does not report a job that jobs.ignore matches", async () => {
    const original = stubOriginalWork();
    const { startTrace } = createAgent({
      jobs: {
        ignore: (job: { queueName: string }) => job.queueName === "emails",
      },
    });
    const handler = vi.fn(async () => undefined);
    work("emails", handler);

    await handlerPassedTo(original)([{ id: "job-1", name: "emails" }]);

    expect(handler).toHaveBeenCalledOnce();
    expect(startTrace).not.toHaveBeenCalled();
  });

  it("wraps the original once when a second agent is created", () => {
    const original = stubOriginalWork();
    createAgent();
    createAgent();

    work("emails", async () => undefined);

    expect(original).toHaveBeenCalledOnce();
  });

  it("hands a transactional worker's db to the handler", async () => {
    const original = stubOriginalWork();
    createAgent();
    const handler = vi.fn(async () => undefined);
    work("emails", { transactional: true }, handler);

    const jobs = [{ id: "job-1", name: "emails" }];
    const db = { executeSql: vi.fn() };
    await (handlerPassedTo(original) as (...args: unknown[]) => unknown)(
      jobs,
      db,
    );

    expect(handler).toHaveBeenCalledWith(jobs, db);
  });

  it("starts the worker outside the context work() was called in", () => {
    const original = stubOriginalWork();
    const { als } = createAgent();
    let storeSeenByPgBoss: unknown = "unset";
    original.mockImplementation(() => {
      storeSeenByPgBoss = als.getStore();
      return "worker-id";
    });

    als.run(new Map([["traceId", "registering-request"]]), () =>
      work("emails", async () => undefined),
    );

    expect(storeSeenByPgBoss).toBeUndefined();
  });

  describe("enqueueing", () => {
    it("adds the active trace id to the trace context pg-boss persists", async () => {
      const { als } = createAgent();
      let persisted: Carrier = null;

      await als.run(new Map([["traceId", "request-trace-1"]]), () =>
        telemetry().send("send", "emails", 1, async (carrier) => {
          persisted = carrier;
          return "job-id";
        }),
      );

      expect(persisted).toEqual({ observeTraceId: "request-trace-1" });
    });

    it("keeps what OpenTelemetry injected beside the trace id", async () => {
      // pg-boss injects a W3C context only with an SDK registered and
      // propagateContext on; this stands in for that carrier, parked where
      // the agent will find it as pg-boss's own send.
      telemetryPrototype[ORIGINAL_SEND] = function (
        this: unknown,
        ...args: unknown[]
      ) {
        const fn = args[3] as (carrier: Carrier) => unknown;
        return fn({ traceparent: "00-otel-span-01" });
      };
      const { als } = createAgent();
      let persisted: Carrier = null;

      await als.run(new Map([["traceId", "request-trace-1"]]), () =>
        telemetry().send("send", "emails", 1, async (carrier) => {
          persisted = carrier;
        }),
      );

      expect(persisted).toEqual({
        traceparent: "00-otel-span-01",
        observeTraceId: "request-trace-1",
      });
    });

    it("leaves a job enqueued outside any trace without a carrier", async () => {
      createAgent();
      let persisted: Carrier = { untouched: "true" };

      await telemetry().send("send", "emails", 1, async (carrier) => {
        persisted = carrier;
      });

      expect(persisted).toBeNull();
    });

    it("warns once, and stamps nothing, when pg-boss telemetry is disabled", async () => {
      const warn = vi.spyOn(Logger.prototype, "warn");
      const { als } = createAgent();
      const persisted: Carrier[] = [];

      await als.run(new Map([["traceId", "request-trace-1"]]), async () => {
        for (let i = 0; i < 2; i++) {
          await telemetry(false).send("send", "emails", 1, async (c) => {
            persisted.push(c);
          });
        }
      });

      expect(persisted).toEqual([null, null]);
      expect(
        warn.mock.calls.filter(([message]) =>
          String(message).includes("openTelemetry.enabled false"),
        ),
      ).toHaveLength(1);
    });
  });

  describe("running", () => {
    const runThroughTelemetry = (
      jobs: { id: string; name: string }[],
      carrierOf: () => TraceCarrier | undefined,
      handler: Handler,
    ) =>
      telemetry().process("emails", jobs, carrierOf, async () => handler(jobs));

    it("runs a job under the trace id it was enqueued with", async () => {
      const original = stubOriginalWork();
      const { als, reported, endTrace } = createAgent();
      let traceId: unknown;
      work("emails", async () => {
        traceId = als.getStore()?.get("traceId");
      });

      await runThroughTelemetry(
        [{ id: "job-1", name: "emails" }],
        () => ({ observeTraceId: "request-trace-1" }),
        handlerPassedTo(original),
      );

      expect(traceId).toBe("request-trace-1");
      await vi.waitFor(() => expect(endTrace).toHaveBeenCalledOnce());
      expect(reported().snapshot).toMatchObject({ id: "job-1" });
      expect(reported().status).toBe("completed");
    });

    it("mints its own id for a carried id that is not a plausible trace id", async () => {
      const original = stubOriginalWork();
      const { als } = createAgent();
      let traceId: unknown;
      work("emails", async () => {
        traceId = als.getStore()?.get("traceId");
      });

      await runThroughTelemetry(
        [{ id: "job-1", name: "emails" }],
        () => ({ observeTraceId: "x".repeat(500) }),
        handlerPassedTo(original),
      );

      expect(traceId).toEqual(expect.any(String));
      expect(traceId).not.toBe("x".repeat(500));
    });

    it("gives a batch a trace of its own, whatever its jobs carried", async () => {
      const original = stubOriginalWork();
      const { als } = createAgent();
      let traceId: unknown;
      work("emails", { batchSize: 2 }, async () => {
        traceId = als.getStore()?.get("traceId");
      });

      await runThroughTelemetry(
        [
          { id: "a", name: "emails" },
          { id: "b", name: "emails" },
        ],
        () => ({ observeTraceId: "request-trace-1" }),
        handlerPassedTo(original),
      );

      expect(traceId).toEqual(expect.any(String));
      expect(traceId).not.toBe("request-trace-1");
    });

    it("runs a job outside whatever trace its worker loop inherited", async () => {
      const original = stubOriginalWork();
      const { als } = createAgent();
      let traceId: unknown;
      work("emails", async () => {
        traceId = als.getStore()?.get("traceId");
      });

      await als.run(new Map([["traceId", "registering-request"]]), () =>
        runThroughTelemetry(
          [{ id: "job-1", name: "emails" }],
          () => undefined,
          handlerPassedTo(original),
        ),
      );

      expect(traceId).toEqual(expect.any(String));
      expect(traceId).not.toBe("registering-request");
    });
  });
});
