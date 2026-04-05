self.addEventListener("install", (event) => {
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.map((key) => caches.delete(key))).then(() => self.registration.unregister())
    )
  );
});

self.addEventListener("fetch", () => {
  // Intentionally no cache handling. This worker exists only to clean up
  // old cache-first registrations from earlier deployments.
});
