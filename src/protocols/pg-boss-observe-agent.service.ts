import { Inject, Injectable, Logger } from "@nestjs/common";
import { AsyncLocalStorage } from "async_hooks";
import { ObserveAgentSharedBuffer } from "../agent/observe-agent.shared-buffer.js";
import {
  JobSnapshot,
  ObserveModuleOptionsWithDefaults,
} from "../interfaces/index.js";
import { OBSERVE_OPTIONS } from "../observe.constants.js";
import { OperationTraceRegistry } from "../services/operation-trace.registry.js";
import { KeyOf } from "../types/key-of.type.js";
import {
  describePeerLoadError,
  loadOptionalPeer,
} from "../utils/optional-peer.util.js";
import { JobRunDescriptor, JobTraceRunner } from "./job-trace-runner.js";

/**
 * The fields of a pg-boss job this agent reads, structurally typed. The dates
 * are only there when the worker asked for `includeMetadata`.
 */
interface PgBossJobLike {
  id?: string;
  name?: string;
  retryCount?: number;
  retryLimit?: number;
  createdOn?: Date | string;
  startAfter?: Date | string;
  startedOn?: Date | string;
}

type PgBossWorkHandler = (jobs: PgBossJobLike[], ...rest: unknown[]) => unknown;

interface PgBossPrototype {
  work?: (name: string, ...args: unknown[]) => unknown;
  [key: symbol]: unknown;
}

interface PgBossModule {
  PgBoss?: { prototype?: PgBossPrototype };
  default?: { prototype?: PgBossPrototype };
}

type TraceCarrier = Record<string, unknown>;

/** The slice of pg-boss's internal `Telemetry` class this agent patches. */
interface TelemetryLike {
  enabled?: boolean;
}

type TelemetrySend = (
  this: TelemetryLike,
  operation: unknown,
  destination: unknown,
  attempted: unknown,
  fn: (carrier: TraceCarrier | null) => unknown,
  ...rest: unknown[]
) => unknown;

type TelemetryProcess = (
  this: TelemetryLike,
  destination: unknown,
  jobs: unknown,
  carrierOf: (job: unknown) => unknown,
  fn: () => unknown,
  ...rest: unknown[]
) => unknown;

interface TelemetryPrototype {
  send?: TelemetrySend;
  process?: TelemetryProcess;
  [key: symbol]: unknown;
}

const ORIGINAL_WORK = Symbol.for("nestjs.observe.pgBoss.work");
const ORIGINAL_SEND = Symbol.for("nestjs.observe.pgBoss.telemetry.send");
const ORIGINAL_PROCESS = Symbol.for("nestjs.observe.pgBoss.telemetry.process");

/**
 * The trace context pg-boss read back with each job it fetched, keyed by the
 * job object it hands the handler. Module-wide so a handler wrapped by one
 * agent still finds what a later agent's `process` patch recorded.
 */
const carriers = new WeakMap<object, TraceCarrier>();

function toTime(value: Date | string | undefined): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  const time = value instanceof Date ? value.getTime() : Date.parse(value);
  return Number.isNaN(time) ? undefined : time;
}

function isCarrier(value: unknown): value is TraceCarrier {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Job tracing for pg-boss, a Postgres-backed queue with no Nest wrapper of its
 * own - applications call `boss.work(queue, handler)` from wherever they like,
 * so the client's `work` is patched rather than any explorer.
 *
 * pg-boss hands a handler an array of jobs. A batch of one - the default -
 * reports as that job; a larger batch is a single handler call, so it reports
 * as one run of the queue, without a job id, under a trace of its own.
 *
 * A job runs under the trace of the operation that enqueued it, as BullMQ's
 * do. pg-boss persists only the options it knows, and the job's `data`
 * belongs to the application, but every job row has a `trace_context` column
 * pg-boss fills from its own telemetry: `send`, `insert`, `flow`, `upsert` and
 * `publish` each hand the row a carrier through `Telemetry.prototype.send`,
 * and a worker reads it back through `Telemetry.prototype.process`. Those two
 * are patched to add the trace id beside whatever OpenTelemetry put there,
 * and to hand it to the run. Retries and dead-letter copies keep the column,
 * so they keep the trace too.
 *
 * pg-boss reads the column back only while its telemetry is enabled - the
 * default. With `openTelemetry: { enabled: false }` each run mints its own
 * trace id, and the agent says so once.
 */
@Injectable()
export class PgBossObserveAgentService<Store extends Record<string, unknown>> {
  private readonly logger = new Logger(PgBossObserveAgentService.name);
  private readonly runner: JobTraceRunner<Store>;
  private warnedTelemetryDisabled = false;

  constructor(
    observeAgentSharedBuffer: ObserveAgentSharedBuffer,
    @Inject(OBSERVE_OPTIONS)
    options: ObserveModuleOptionsWithDefaults,
    operationTraceRegistry: OperationTraceRegistry,
    private readonly asyncLocalStorage: AsyncLocalStorage<
      Map<KeyOf<Store>, any>
    >,
  ) {
    this.runner = new JobTraceRunner(
      observeAgentSharedBuffer,
      options,
      operationTraceRegistry,
      asyncLocalStorage,
      this.logger,
    );
    this.patchPgBoss();
  }

  private loadPgBossPrototype(): PgBossPrototype | null | undefined {
    const result = loadOptionalPeer<PgBossModule>("pg-boss");
    if (!result.installed) {
      return undefined;
    }
    if (result.error) {
      this.logger.warn(
        `pg-boss is installed but could not be loaded, so its jobs will not be instrumented: ${describePeerLoadError(result.error)}`,
      );
      return null;
    }
    // v12 exports the class by name; earlier majors as the default export.
    return (
      result.module?.PgBoss?.prototype ??
      result.module?.default?.prototype ??
      null
    );
  }

  /**
   * pg-boss's internal `Telemetry` class. Not public API, so anything other
   * than the shape this agent was written against is reported, and jobs are
   * still traced - each under an id of its own.
   */
  private loadTelemetryPrototype(): TelemetryPrototype | undefined {
    const result = loadOptionalPeer<{
      default?: { prototype?: TelemetryPrototype };
    }>("pg-boss", "pg-boss/dist/telemetry.js");
    const prototype = result.installed
      ? result.module?.default?.prototype
      : undefined;
    if (
      typeof prototype?.send !== "function" ||
      typeof prototype.process !== "function"
    ) {
      const cause =
        result.installed && result.error
          ? `: ${describePeerLoadError(result.error)}`
          : "";
      this.logger.warn(
        `pg-boss's telemetry is not where this agent expects it, so its jobs will not carry the enqueuing trace id${cause}.`,
      );
      return undefined;
    }
    return prototype;
  }

  private readQueueMetadata(job: PgBossJobLike): Partial<JobSnapshot> {
    const metadata: Partial<JobSnapshot> = {};

    const createdOn = toTime(job.createdOn);
    if (createdOn !== undefined) {
      metadata.enqueuedAt = new Date(createdOn).toISOString();
      const startedOn = toTime(job.startedOn) ?? Date.now();
      // As with BullMQ: a `startAfter` the caller asked for is a schedule,
      // not a backlog, so the wait is measured from when the job became
      // runnable.
      const runnableAt = Math.max(createdOn, toTime(job.startAfter) ?? 0);
      metadata.waitDuration = Math.max(0, startedOn - runnableAt);
    }
    if (typeof job.retryCount === "number") {
      metadata.attemptsMade = job.retryCount;
    }
    if (typeof job.retryLimit === "number") {
      // The limit counts retries; the first attempt is not one of them.
      metadata.maxAttempts = job.retryLimit + 1;
    }
    return metadata;
  }

  private describe(queueName: string, jobs: unknown): JobRunDescriptor {
    const batch = Array.isArray(jobs) ? (jobs as PgBossJobLike[]) : [];
    const only = batch.length === 1 ? batch[0] : undefined;
    return {
      queueName,
      name: queueName,
      id: only?.id,
      // A batch is one run over jobs that may each have come from a
      // different trace, so it inherits none of them.
      opts: only && typeof only === "object" ? carriers.get(only) : undefined,
      metadata: only ? this.readQueueMetadata(only) : {},
    };
  }

  private patchPgBoss() {
    const prototype = this.loadPgBossPrototype();
    if (prototype === undefined) {
      // An optional peer. No pg-boss means no worker to wrap.
      return;
    }
    const originalWork = (prototype?.[ORIGINAL_WORK] ??
      prototype?.work) as PgBossPrototype["work"];
    if (!prototype || typeof originalWork !== "function") {
      this.logger.warn(
        "pg-boss's PgBoss.prototype.work is not available, so its jobs will not be instrumented.",
      );
      return;
    }
    prototype[ORIGINAL_WORK] = originalWork;

    const instrument = (queueName: string, handler: PgBossWorkHandler) =>
      this.instrument(queueName, handler);
    const als = this.asyncLocalStorage;
    // work(name, handler) | work(name, options, handler)
    prototype.work = function (
      this: unknown,
      name: string,
      ...args: unknown[]
    ) {
      const last = args.length - 1;
      if (typeof name === "string" && typeof args[last] === "function") {
        args[last] = instrument(name, args[last] as PgBossWorkHandler);
      }
      // The worker's polling loop starts here and would otherwise carry the
      // caller's context for as long as it runs: every fetch recorded as a
      // query of whichever request happened to register the worker, and
      // every job folded into that request's trace instead of its own.
      return als.exit(() => originalWork.call(this, name, ...args));
    };

    this.patchTelemetry();
  }

  private patchTelemetry() {
    const prototype = this.loadTelemetryPrototype();
    if (!prototype) {
      return;
    }
    const originalSend = (prototype[ORIGINAL_SEND] ??
      prototype.send) as TelemetrySend;
    const originalProcess = (prototype[ORIGINAL_PROCESS] ??
      prototype.process) as TelemetryProcess;
    prototype[ORIGINAL_SEND] = originalSend;
    prototype[ORIGINAL_PROCESS] = originalProcess;

    const stamp = () => this.runner.traceFieldsToHandOn();
    const warnTelemetryDisabled = () => this.warnTelemetryDisabled();
    const als = this.asyncLocalStorage;

    // send(operation, destination, attempted, fn(carrier), idsOf?)
    prototype.send = function (
      this: TelemetryLike,
      operation,
      destination,
      attempted,
      fn,
      ...rest
    ) {
      // Read here, in the enqueuing caller's context, before pg-boss opens a
      // span of its own around `fn`.
      const fields = stamp();
      if (!fields || typeof fn !== "function") {
        return originalSend.call(
          this,
          operation,
          destination,
          attempted,
          fn,
          ...rest,
        );
      }
      if (this.enabled === false) {
        // Written or not, the column is not read back with telemetry off.
        warnTelemetryDisabled();
        return originalSend.call(
          this,
          operation,
          destination,
          attempted,
          fn,
          ...rest,
        );
      }
      return originalSend.call(
        this,
        operation,
        destination,
        attempted,
        (carrier: TraceCarrier | null) =>
          // Beside, never over, what OpenTelemetry injected.
          fn({ ...fields, ...(isCarrier(carrier) ? carrier : {}) }),
        ...rest,
      );
    };

    // process(destination, jobs, carrierOf, fn)
    prototype.process = function (
      this: TelemetryLike,
      destination,
      jobs,
      carrierOf,
      fn,
      ...rest
    ) {
      if (Array.isArray(jobs) && typeof carrierOf === "function") {
        for (const job of jobs as unknown[]) {
          const carrier: unknown =
            typeof job === "object" && job !== null
              ? carrierOf(job)
              : undefined;
          if (isCarrier(carrier)) {
            carriers.set(job as object, carrier);
          }
        }
      }
      // As pg-boss parents its own process span on the root context: a run
      // has nothing to do with whatever context its worker loop inherited.
      // This also covers pg-boss's internal cron worker, which `work` above
      // never sees, so a scheduled job is not stamped with a stray trace.
      return als.exit(() =>
        originalProcess.call(this, destination, jobs, carrierOf, fn, ...rest),
      );
    };
  }

  private warnTelemetryDisabled() {
    if (this.warnedTelemetryDisabled) {
      return;
    }
    this.warnedTelemetryDisabled = true;
    this.logger.warn(
      "pg-boss was created with openTelemetry.enabled false, so it does not read back the trace context its jobs carry. Its jobs will each start a trace of their own instead of continuing the one that enqueued them.",
    );
  }

  private instrument(
    queueName: string,
    handler: PgBossWorkHandler,
  ): PgBossWorkHandler {
    const runner = this.runner;
    const describe = (jobs: unknown) => this.describe(queueName, jobs);
    // Every argument goes through: a transactional worker is handed the
    // transaction's db beside the jobs.
    return function (this: unknown, ...args: [PgBossJobLike[], ...unknown[]]) {
      return runner.run(describe(args[0]), () => handler.apply(this, args));
    };
  }
}
