const TARGET = "https://nse-proxy-mojx.onrender.com/ping";

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(ping());
  },
};

async function ping() {
  const controller = new AbortController();
  // 90s wall-clock cap — Render cold start is ~50s; this is the same as our curl --max-time 90
  const timer = setTimeout(() => controller.abort(), 90_000);
  try {
    const res = await fetch(TARGET, {
      signal: controller.signal,
      headers: { "User-Agent": "cf-keepalive/1.0" },
    });
    console.log(`ping ok: ${res.status}`);
  } catch (err) {
    console.error(`ping failed: ${err.message}`);
  } finally {
    clearTimeout(timer);
  }
}
