import { describe, expect, it } from 'vitest';
import worker, { type Env } from '../src/index.ts';

function stubFetcher(name: string): Fetcher {
  return {
    fetch: async () => new Response(name),
  } as unknown as Fetcher;
}

function makeEnv(overrides: Partial<Env> = {}): Env {
  return {
    WORKSHOP_BACKEND: stubFetcher('backend'),
    ...overrides,
  };
}

async function route(env: Env, path: string): Promise<string> {
  const response = await worker.fetch(new Request(`https://example.test${path}`), env, {} as ExecutionContext);
  return await response.text();
}

describe('router', () => {
  it('routes /api and screenshot prefixes to the workshop backend', async () => {
    const env = makeEnv({ ASSETS: stubFetcher('assets') });
    expect(await route(env, '/api')).toBe('backend');
    expect(await route(env, '/api/rpc')).toBe('backend');
    expect(await route(env, '/blueprint-screenshot/x')).toBe('backend');
    expect(await route(env, '/application-screenshot/x')).toBe('backend');
    expect(await route(env, '/apps/hormuz-ship-watch/')).toBe('backend');
  });

  it('serves everything else from ASSETS when the binding is present', async () => {
    const env = makeEnv({ ASSETS: stubFetcher('assets') });
    expect(await route(env, '/')).toBe('assets');
    expect(await route(env, '/integrations')).toBe('assets');
  });

  it('falls through to the backend when ASSETS is absent', async () => {
    const env = makeEnv();
    expect(await route(env, '/')).toBe('backend');
  });
});
