import { api } from './client';
import { endpoints, config } from '@/config';
import {
  ExecuteQueryRequest,
  QueryResult,
  QueryHistoryItem,
  PaginatedResponse,
} from '@/types';

export const queriesApi = {
  /**
   * Execute a natural language query
   */
  execute: async (request: ExecuteQueryRequest): Promise<QueryResult> => {
    return api.post<QueryResult>(endpoints.queries.execute, request);
  },

  /**
   * Get query execution history
   */
  getHistory: async (
    page = 1,
    pageSize = config.app.queryHistoryPageSize
  ): Promise<PaginatedResponse<QueryHistoryItem>> => {
    return api.get<PaginatedResponse<QueryHistoryItem>>(
      endpoints.queries.history,
      {
        params: { page, page_size: pageSize },
      }
    );
  },

  /**
   * Toggle bookmark on a query
   */
  toggleBookmark: async (queryId: number): Promise<{ is_bookmarked: boolean }> => {
    return api.post<{ is_bookmarked: boolean }>(
      endpoints.queries.bookmark(queryId)
    );
  },
};

/**
 * WebSocket connection for streaming query execution
 */
export class QueryWebSocket {
  private ws: WebSocket | null = null;
  private url: string;
  private token: string;

  constructor(token: string) {
    this.token = token;
    const wsUrl = config.wsBaseUrl.replace(/^http/, 'ws');
    this.url = `${wsUrl}${config.apiPrefix}${endpoints.queries.websocket}`;
  }

  /**
   * Connect and execute query with streaming updates
   */
  async execute(
    request: ExecuteQueryRequest,
    onProgress: (stage: string, message: string, percent?: number) => void,
    onResult: (result: QueryResult) => void,
    onError: (error: string) => void
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(this.url);

      this.ws.onopen = () => {
        // Send authentication and query request
        this.ws?.send(
          JSON.stringify({
            token: this.token,
            ...request,
          })
        );
      };

      this.ws.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);

          if (message.type === 'progress') {
            const { stage, message: msg, progress_percent } = message.data;
            onProgress(stage, msg, progress_percent);
          } else if (message.type === 'result') {
            onResult(message.data);
            this.close();
            resolve();
          } else if (message.type === 'error') {
            onError(message.data.message);
            this.close();
            reject(new Error(message.data.message));
          }
        } catch (error) {
          console.error('Error parsing WebSocket message:', error);
          onError('Failed to parse server response');
          this.close();
          reject(error);
        }
      };

      this.ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        onError('WebSocket connection error');
        this.close();
        reject(error);
      };

      this.ws.onclose = () => {
        resolve();
      };
    });
  }

  /**
   * Close the WebSocket connection
   */
  close(): void {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }
}
