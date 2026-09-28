let activeFile = null;
let activeManifest = null;
let activeKey = null;
let chunkOffsets = [];

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));

self.addEventListener("message", (event) => {
  if (event.data && event.data.type === "REGISTER_STREAM") {
    activeFile = event.data.file;
    activeManifest = event.data.manifest;
    activeKey = event.data.key;

    let offset = event.data.clusterOffset;
    chunkOffsets = new Array(activeManifest.total);
    for (let i = 0; i < activeManifest.total; i++) {
      chunkOffsets[i] = offset;
      offset += activeManifest.sizes[i];
    }
  }
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);

  if (url.pathname.endsWith("/stream-virtual-playback.mp4")) {
    event.respondWith(handleRangeRequest(event.request));
  }
});

async function handleRangeRequest(request) {
  if (!activeFile || !activeManifest || !activeKey) {
    return new Response("Media stream not initialized", { status: 404 });
  }

  // Calculate actual decrypted size across all chunks
  // Each chunk overhead is 28 bytes: 12-byte IV + 16-byte GCM authentication tag
  let totalDecryptedSize = 0;
  for (let s of activeManifest.sizes) {
    totalDecryptedSize += (s - 28);
  }

  const rangeHeader = request.headers.get("range");
  let start = 0;
  let end = totalDecryptedSize - 1;

  if (rangeHeader) {
    const match = rangeHeader.match(/bytes=(\d+)-(\d*)/);
    if (match) {
      start = parseInt(match[1], 10);
      if (match[2]) end = parseInt(match[2], 10);
    }
  }

  const stream = new ReadableStream({
    async start(controller) {
      try {
        let currentPos = 0;
        for (let i = 0; i < activeManifest.total; i++) {
          const decChunkSize = activeManifest.sizes[i] - 28;
          const chunkStart = currentPos;
          const chunkEnd = currentPos + decChunkSize - 1;
          currentPos += decChunkSize;

          if (chunkEnd >= start && chunkStart <= end) {
            const sliceStart = chunkOffsets[i];
            const sliceEnd = sliceStart + activeManifest.sizes[i];
            const rawChunk = await activeFile.slice(sliceStart, sliceEnd).arrayBuffer();

            const u8 = new Uint8Array(rawChunk);
            const iv = u8.subarray(0, 12);
            const ciphertext = u8.subarray(12);

            const decryptedBuf = await crypto.subtle.decrypt(
              { name: "AES-GCM", iv: iv },
              activeKey,
              ciphertext
            );

            let subStart = Math.max(0, start - chunkStart);
            let subEnd = Math.min(decChunkSize, end - chunkStart + 1);

            const chunkSlice = new Uint8Array(decryptedBuf).subarray(subStart, subEnd);
            controller.enqueue(chunkSlice);
          }

          if (currentPos > end) break;
        }
        controller.close();
      } catch (err) {
        controller.error(err);
      }
    }
  });

  return new Response(stream, {
    status: rangeHeader ? 206 : 200,
    headers: {
      "Content-Type": activeManifest.mime || "video/mp4",
      "Content-Range": `bytes ${start}-${end}/${totalDecryptedSize}`,
      "Accept-Ranges": "bytes",
      "Content-Length": (end - start + 1).toString()
    }
  });
}
