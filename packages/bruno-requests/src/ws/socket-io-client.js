import { io } from 'socket.io-client';
import { hexy as hexdump } from 'hexy';
import { getParsedWsUrlObject } from './ws-url';

const normalizeMessageByFormat = (message, format) => {
  if (!message) {
    return '';
  }

  switch (format) {
    case 'json':
      if (typeof message === 'string') {
        return message;
      }
      return JSON.stringify(message);
    case 'raw':
    case 'xml':
      return message;
    default: {
      if (typeof message === 'string') {
        return message;
      }
      if (typeof message === 'object') {
        return JSON.stringify(message);
      }
      return '';
    }
  }
};

const toStringForHexdump = (payload) => {
  if (payload === undefined || payload === null) {
    return '';
  }
  if (typeof payload === 'string') {
    return payload;
  }
  try {
    return JSON.stringify(payload);
  } catch {
    return String(payload);
  }
};

const normalizeSocketIoUrl = (url) => {
  const parsedUrl = getParsedWsUrlObject(url);
  const fullUrl = parsedUrl.fullUrl || url;
  if (!fullUrl) {
    return '';
  }

  try {
    const urlObj = new URL(fullUrl);
    if (urlObj.protocol === 'ws:') {
      urlObj.protocol = 'http:';
    }
    if (urlObj.protocol === 'wss:') {
      urlObj.protocol = 'https:';
    }
    return urlObj.href;
  } catch {
    return fullUrl;
  }
};

const createSequencer = () => {
  const seq = {};

  const nextSeq = (requestId, collectionId) => {
    seq[requestId] ||= {};
    seq[requestId][collectionId] ||= 0;
    return ++seq[requestId][collectionId];
  };

  const clean = (requestId, collectionId = undefined) => {
    if (collectionId) {
      delete seq[requestId][collectionId];
    }
    if (seq[requestId] && !Object.keys(seq[requestId]).length) {
      delete seq[requestId];
    }
  };

  return {
    next: nextSeq,
    clean
  };
};

const seq = createSequencer();

class SocketIoClient {
  messageQueues = {};
  activeConnections = new Map();
  pendingCloseMeta = new Map();

  constructor(eventCallback) {
    this.eventCallback = eventCallback;
  }

  async startConnection({ request, collection, options = {} }) {
    const { url, headers = {} } = request;
    const { timeout = 30000, sslOptions = {} } = options;
    const timeoutAsNumber = Number(timeout);
    const validTimeout = isNaN(timeoutAsNumber) ? 30000 : timeoutAsNumber;

    const requestId = request.uid;
    const collectionUid = collection.uid;

    try {
      const socketConnection = io(normalizeSocketIoUrl(url), {
        autoConnect: true,
        forceNew: true,
        timeout: validTimeout,
        extraHeaders: headers,
        rejectUnauthorized: sslOptions.rejectUnauthorized,
        ca: sslOptions.ca,
        cert: sslOptions.cert,
        key: sslOptions.key,
        pfx: sslOptions.pfx,
        passphrase: sslOptions.passphrase
      });

      this.#setupSocketEventHandlers(socketConnection, requestId, collectionUid);
      this.#addConnection(requestId, collectionUid, socketConnection);
      this.eventCallback('main:ws:connecting', requestId, collectionUid);
      return socketConnection;
    } catch (error) {
      this.eventCallback('main:ws:error', requestId, collectionUid, {
        error: error.message
      });
      throw error;
    }
  }

  #getMessageQueueId(requestId) {
    return `${requestId}`;
  }

  queueMessage(requestId, collectionUid, message, format = 'raw', options = {}) {
    const connectionMeta = this.activeConnections.get(requestId);

    const mqKey = this.#getMessageQueueId(requestId);
    this.messageQueues[mqKey] ||= [];
    this.messageQueues[mqKey].push({
      message,
      format,
      options
    });

    if (connectionMeta?.connection?.connected) {
      this.#flushQueue(requestId, collectionUid);
    }
  }

  #flushQueue(requestId, collectionUid) {
    const mqKey = this.#getMessageQueueId(requestId);
    if (!(mqKey in this.messageQueues)) return;

    while (this.messageQueues[mqKey].length > 0) {
      const { message, format, options } = this.messageQueues[mqKey].shift();
      this.sendMessage(requestId, collectionUid, message, format, options);
    }
  }

  sendMessage(requestId, collectionUid, message, format = 'raw', options = {}) {
    const connectionMeta = this.activeConnections.get(requestId);
    const socketConnection = connectionMeta?.connection;

    if (!socketConnection?.connected) {
      this.eventCallback('main:ws:error', requestId, collectionUid, {
        error: 'Socket.IO connection not available or not open'
      });
      return;
    }

    const payload = normalizeMessageByFormat(message, format);
    const eventName = String(options?.eventName || 'message').trim() || 'message';
    socketConnection.emit(eventName, payload);

    this.eventCallback('main:ws:message', requestId, collectionUid, {
      message: {
        event: eventName,
        data: payload
      },
      messageHexdump: hexdump(Buffer.from(toStringForHexdump(payload))),
      type: 'outgoing',
      seq: seq.next(requestId, collectionUid),
      timestamp: Date.now()
    });
  }

  close(requestId, code = 1000, reason = 'Client initiated close') {
    const connectionMeta = this.activeConnections.get(requestId);
    if (connectionMeta?.connection) {
      this.pendingCloseMeta.set(requestId, { code, reason });
      connectionMeta.connection.disconnect();
    }
  }

  isConnectionActive(requestId) {
    const connectionMeta = this.activeConnections.get(requestId);
    return !!connectionMeta?.connection?.connected;
  }

  getActiveConnectionIds() {
    return Array.from(this.activeConnections.keys());
  }

  closeForCollection(collectionUid) {
    [...this.activeConnections.keys()].forEach((requestId) => {
      const meta = this.activeConnections.get(requestId);
      if (meta?.collectionUid === collectionUid) {
        this.close(requestId);
      }
    });
  }

  clearAllConnections() {
    const connectionIds = this.getActiveConnectionIds();
    this.activeConnections.forEach((_meta, requestId) => {
      this.close(requestId, 1000, 'Client clearing all connections');
    });

    if (connectionIds.length > 0) {
      this.eventCallback('main:ws:connections-changed', {
        type: 'cleared',
        activeConnectionIds: []
      });
    }
  }

  #setupSocketEventHandlers(socket, requestId, collectionUid) {
    socket.on('connect', () => {
      this.#flushQueue(requestId, collectionUid);
      this.eventCallback('main:ws:open', requestId, collectionUid, {
        timestamp: Date.now(),
        url: socket.io.uri,
        seq: seq.next(requestId, collectionUid)
      });
    });

    socket.onAny((eventName, ...args) => {
      if (['connect', 'disconnect', 'connect_error', 'error'].includes(eventName)) {
        return;
      }
      const payload = args.length <= 1 ? args[0] : args;
      this.eventCallback('main:ws:message', requestId, collectionUid, {
        message: {
          event: eventName,
          data: payload
        },
        messageHexdump: hexdump(Buffer.from(toStringForHexdump(payload))),
        type: 'incoming',
        seq: seq.next(requestId, collectionUid),
        timestamp: Date.now()
      });
    });

    socket.on('connect_error', (error) => {
      this.eventCallback('main:ws:error', requestId, collectionUid, {
        error: error?.message || 'Socket.IO connection error',
        seq: seq.next(requestId, collectionUid),
        timestamp: Date.now()
      });
    });

    socket.on('error', (error) => {
      this.eventCallback('main:ws:error', requestId, collectionUid, {
        error: error?.message || String(error),
        seq: seq.next(requestId, collectionUid),
        timestamp: Date.now()
      });
    });

    socket.on('disconnect', (reason) => {
      const closeMeta = this.pendingCloseMeta.get(requestId);
      this.pendingCloseMeta.delete(requestId);

      this.eventCallback('main:ws:close', requestId, collectionUid, {
        code: closeMeta?.code ?? 1000,
        reason: closeMeta?.reason || reason || 'Disconnected',
        seq: seq.next(requestId, collectionUid),
        timestamp: Date.now()
      });
      seq.clean(requestId, collectionUid);
      this.#removeConnection(requestId);
    });
  }

  #addConnection(requestId, collectionUid, connection) {
    this.activeConnections.set(requestId, { collectionUid, connection });
    this.eventCallback('main:ws:connections-changed', {
      type: 'added',
      requestId,
      seq: seq.next(requestId, collectionUid),
      activeConnectionIds: this.getActiveConnectionIds()
    });
  }

  #removeConnection(requestId) {
    const mqId = this.#getMessageQueueId(requestId);
    if (mqId in this.messageQueues) {
      this.messageQueues[mqId] = [];
    }

    if (this.activeConnections.has(requestId)) {
      this.activeConnections.delete(requestId);
      this.eventCallback('main:ws:connections-changed', {
        type: 'removed',
        requestId,
        activeConnectionIds: this.getActiveConnectionIds()
      });
    }
  }

  connectionStatus(requestId) {
    const connectionMeta = this.activeConnections.get(requestId);
    if (connectionMeta?.connection?.connected) return 'connected';
    if (connectionMeta?.connection?.active || connectionMeta?.connection?.io?._readyState === 'opening') return 'connecting';
    return 'disconnected';
  }
}

export { SocketIoClient };
