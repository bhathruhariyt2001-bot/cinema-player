let channelPort = null;
let activeManifest = null;

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (e) => e.waitUntil(self.clients.claim()));

self.addEventListener("message", (event) => {
  if (event.data && event.data.type === "REGISTER_CHANNEL") {
    channelPort = event.ports[0];
    activeManifest = event.data.manifest;

    channelPort.onmessage = (e) => {
      // Keep channel alive
    };
  }
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  if (url.pathname.endsWith("/stream-virtual-playback.mp4")) {
    event.respondWith(handleStream(event.request));
  }
});

async function handleStream(request) {
  if (!activeManifest || !channelPort) {
    return new Response("Not initialized", { status: 404 });
  }

  let totalSize = 0;
  for (let s of activeManifest.sizes) totalSize += (s - 28);

  const range = request.headers.get("range");
  let start = 0;
  let end = totalSize - 1;

  if (range) {
    const m = range.match(/bytes=(\d+)-(\d*)/);
    if (m) {
      start = parseInt(m[1], 10);
      if (m[2]) end = parseInt(m[2], 10);
    }
  }

  // Request a small window directly from the main thread (max 8MB per read)
  const stream = new ReadableStream({
    async start(controller) {
      const CHUNK_WINDOW = 8 * 1024 * 1024;
      let cur = start;
      const targetEnd = Math.min(end, start + CHUNK_WINDOW);

      try {
        const bytes = await requestBytesFromMain(cur, targetEnd);
        if (bytes && bytes.length > 0) {
          controller.enqueue(bytes);
        }
        controller.close();
      } catch (err) {
        controller.error(err);
      }
    }
  });

  return new Response(stream, {
    status: range ? 206 : 200,
    headers: {
      "Content-Type": activeManifest.mime || "video/mp4",
      "Content-Range": `bytes ${start}-${end}/${totalSize}`,
      "Accept-Ranges": "bytes",
      "Content-Length": (end - start + 1).toString()
    }
  });
}

function requestBytesFromMain(start, end) {
  return new Promise((resolve, reject) => {
    const msgId = Math.random().toString(36).slice(2);
    const handler = (e) => {
      if (e.data.id === msgId) {
        channelPort.removeEventListener("message", handler);
        if (e.data.error) reject(new Error(e.data.error));
        else resolve(new Uint8Array(e.data.buffer));
      }
    };
    channelPort.addEventListener("message", handler);
    channelPort.start();
    channelPort.postMessage({ type: "READ_CHUNK", id: msgId, start, end });
  });
}
