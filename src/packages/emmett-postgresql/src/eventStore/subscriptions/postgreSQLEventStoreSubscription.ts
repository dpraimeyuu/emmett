import { dumbo, type SQLExecutor } from '@event-driven-io/dumbo';
import {
  EmmettError,
  type Event,
  type ReadEvent,
  type ReadEventMetadataWithGlobalPosition,
} from '@event-driven-io/emmett';
import { setTimeout } from 'timers';
import {
  readMessagesBatch as originalReadMessagesBatch,
  readMessagesBatch,
  type ReadMessagesBatchOptions,
} from '../schema/readMessagesBatch';

export type PostgreSQLEventStoreSubscription = {
  isRunning: boolean;
  subscribe: () => Promise<void>;
  stop: () => Promise<void>;
};

export const PostgreSQLEventStoreSubscription = {
  result: {
    skip: (options?: {
      reason?: string;
    }): PostgreSQLEventStoreSubscriptionMessageHandlerResult => ({
      type: 'SKIP',
      ...(options ?? {}),
    }),
    stop: (options?: {
      reason?: string;
      error?: EmmettError;
    }): PostgreSQLEventStoreSubscriptionMessageHandlerResult => ({
      type: 'STOP',
      ...(options ?? {}),
    }),
  },
};

export type PostgreSQLEventStoreSubscriptionMessageHandlerResult =
  | void
  | { type: 'SKIP'; reason?: string }
  | { type: 'STOP'; reason?: string; error?: EmmettError };

export type PostgreSQLEventStoreSubscriptionEachMessageHandler<
  EventType extends Event = Event,
> = (
  event: ReadEvent<EventType, ReadEventMetadataWithGlobalPosition>,
) =>
  | Promise<PostgreSQLEventStoreSubscriptionMessageHandlerResult>
  | PostgreSQLEventStoreSubscriptionMessageHandlerResult;

export const DefaultPostgreSQLEventStoreSubscriptionBatchSize = 100;

export type PostgreSQLEventStoreSubscriptionOptions<
  EventType extends Event = Event,
> = {
  connectionString: string;
  eachMessage: PostgreSQLEventStoreSubscriptionEachMessageHandler<EventType>;
  batchSize?: number;
};

type MessageBatchPoolerOptions<EventType extends Event = Event> = {
  readMessagesBatch: (options: ReadMessagesBatchOptions & { partition?: string }) => ReturnType<typeof originalReadMessagesBatch>;
  batchSize: number;
  eachMessage: PostgreSQLEventStoreSubscriptionOptions<EventType>['eachMessage'];
};

type MessageBatchPoolerOptionsOskar<EventType extends Event = Event> = {
  executor: SQLExecutor;
  batchSize: number;
  eachMessage: PostgreSQLEventStoreSubscriptionOptions<EventType>['eachMessage'];
};


const messageBatchPooler = <EventType extends Event = Event>({
  readMessagesBatch,
  batchSize,
  eachMessage,
}: MessageBatchPoolerOptions<EventType>) => {
  let isRunning = false;

  let start: Promise<void>;

  const pollMessages = async () => {
    const options: ReadMessagesBatchOptions = { from: 0n, batchSize };

    let waitTime = 100;

    do {
      const { events, currentGlobalPosition, areEventsLeft } =
        await readMessagesBatch(options);

      for (const message of events) {
        const result = await eachMessage(
          message as ReadEvent<EventType, ReadEventMetadataWithGlobalPosition>,
        );

        if (result) {
          if (result.type === 'SKIP') continue;
          else if (result.type === 'STOP') {
            isRunning = false;
            break;
          }
        }
      }
      options.from = currentGlobalPosition;

      if (!areEventsLeft) {
        waitTime = Math.min(waitTime * 2, 5000);
        await new Promise((resolve) => setTimeout(resolve, waitTime));
      } else {
        waitTime = 0;
      }
    } while (isRunning);
  };

  return {
    get isRunning() {
      return isRunning;
    },
    start: () => {
      start = (async () => {
        isRunning = true;

        return pollMessages();
      })();

      return start;
    },
    stop: async () => {
      isRunning = false;
      await start;
    },
  };
};

const messageBatchPoolerOskar = <EventType extends Event = Event>({
  executor,
  batchSize,
  eachMessage,
}: MessageBatchPoolerOptionsOskar<EventType>) => {
  let isRunning = false;

  let start: Promise<void>;

  const pollMessages = async () => {
    const options: ReadMessagesBatchOptions = { from: 0n, batchSize };

    let waitTime = 100;

    do {
      const { events, currentGlobalPosition, areEventsLeft } =
        await readMessagesBatch(executor, options);

      for (const message of events) {
        const result = await eachMessage(
          message as ReadEvent<EventType, ReadEventMetadataWithGlobalPosition>,
        );

        if (result) {
          if (result.type === 'SKIP') continue;
          else if (result.type === 'STOP') {
            isRunning = false;
            break;
          }
        }
      }
      options.from = currentGlobalPosition;

      if (!areEventsLeft) {
        waitTime = Math.min(waitTime * 2, 5000);
        await new Promise((resolve) => setTimeout(resolve, waitTime));
      } else {
        waitTime = 0;
      }
    } while (isRunning);
  };

  return {
    get isRunning() {
      return isRunning;
    },
    start: () => {
      start = (async () => {
        isRunning = true;

        return pollMessages();
      })();

      return start;
    },
    stop: async () => {
      isRunning = false;
      await start;
    },
  };
};

interface IExecuteSql {
  execute: SQLExecutor
}

interface ICanBeClosed {
  close: () => Promise<void>
}

interface ConnectionPool extends IExecuteSql, ICanBeClosed {}

type CreateConnectionPool = (options: { connectionString: string }) => ConnectionPool

type CreatePostgresSQLEventStoreSubscription<EventType extends Event = Event> = (options: PostgreSQLEventStoreSubscriptionOptions<EventType>) => PostgreSQLEventStoreSubscription

export type CreateMessageBatchPoolingBasedPostgresSqlEventStoreSubscription = <EventType extends Event = Event>(context: {
  createConnectionPool: CreateConnectionPool,
  readBatchMessages: typeof originalReadMessagesBatch
}) => CreatePostgresSQLEventStoreSubscription<EventType>

export const createMessageBatchPoolingPostgresSqlEventStoreSubscription: CreateMessageBatchPoolingBasedPostgresSqlEventStoreSubscription = ({ createConnectionPool, readBatchMessages }) => (options) => {
    let isRunning = false;
    const { connectionString } = options;
    const pool = createConnectionPool({ connectionString })
    const messagePooler = messageBatchPooler({
      readMessagesBatch: (options) => readBatchMessages(pool.execute, options),
      eachMessage: options.eachMessage,
      batchSize:
        options.batchSize ?? DefaultPostgreSQLEventStoreSubscriptionBatchSize,
    });
  
    let subscribe: Promise<void>;
  
    return {
      get isRunning() {
        return isRunning;
      },
      subscribe: () => {
        subscribe = (() => {
          isRunning = true;
  
          return messagePooler.start();
        })();
  
        return subscribe;
      },
      stop: async () => {
        await messagePooler.stop();
        await subscribe;
        await pool.close();
        isRunning = false;
      },
    };
};

const createPoolWithDumbo: CreateConnectionPool = (options) => {
  const dumboPool = dumbo(options)

  return {
    execute: dumboPool.execute,
    close: dumboPool.close
  }
}

const postgreSQLEventStoreBatchPoolingSubscription = createMessageBatchPoolingPostgresSqlEventStoreSubscription({
  createConnectionPool: createPoolWithDumbo,
  readBatchMessages: originalReadMessagesBatch
});

// To Twoje 👇
const postgreSQLEventStoreSubscriptionOskar = <
  EventType extends Event = Event,
>(
  options: PostgreSQLEventStoreSubscriptionOptions<EventType>,
): PostgreSQLEventStoreSubscription => {
  let isRunning = false;

  const { connectionString } = options;
  const pool = dumbo({ connectionString });
  const messagePooler = messageBatchPoolerOskar({
    executor: pool.execute,
    eachMessage: options.eachMessage,
    batchSize:
      options.batchSize ?? DefaultPostgreSQLEventStoreSubscriptionBatchSize,
  });

  let subscribe: Promise<void>;

  return {
    get isRunning() {
      return isRunning;
    },
    subscribe: () => {
      subscribe = (() => {
        isRunning = true;

        return messagePooler.start();
      })();

      return subscribe;
    },
    stop: async () => {
      await messagePooler.stop();
      await subscribe;
      await pool.close();
      isRunning = false;
    },
  };
};

type OskarSubscription = typeof postgreSQLEventStoreSubscriptionOskar
type NewSubscription = typeof postgreSQLEventStoreBatchPoolingSubscription

export function assert<T extends never>() {}
type TypeEqualityGuard<A,B> = Exclude<A,B> | Exclude<B,A>;

assert<TypeEqualityGuard<OskarSubscription, NewSubscription>>()
export const postgreSQLEventStoreSubscription: CreatePostgresSQLEventStoreSubscription = postgreSQLEventStoreBatchPoolingSubscription 
// tu mamy zapewniony kontrakt, nieważne czy użyjemy
// postgreSQLEventStoreBatchPoolingSubscription czy postgreSQLEventStoreSubscriptionOskar
// zwykły odbiorca nie wie co dostaje, bo to jest chowane w module
// i widzi to jako postgreSQLEventStoreSubscription
// ale nadal może zrobić import createMessageBatchPoolingPostgresSqlEventStoreSubscription
// i stworzyć sobie swój postgreSQLEventStoreBatchPoolingSubscription
