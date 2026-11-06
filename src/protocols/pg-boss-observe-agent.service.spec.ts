import { AsyncLocalStorage } from "async_hooks";
import { PgBoss } from "pg-boss";
import { OperationTraceRegistry } from "../services/operation-trace.registry.js";
import { PgBossObserveAgentService } from "./pg-boss-observe-agent.service.js";

type Handler = (jobs: unknown[]) => unknown;

const ORIGINAL_WORK = Symbol.for("nestjs.observe.pgBoss.work");
const prototype = PgBoss.prototype as unknown as Record<
  string | symbol,
  unknown
>;
const realWork = prototype.work;

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
});
