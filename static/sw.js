self.addEventListener('push', event => {
  const d = event.data ? event.data.json() : { title: '주식 알림', body: '' };
  event.waitUntil(
    self.registration.showNotification(d.title, {
      body: d.body,
      icon: '/icon.svg',
      badge: '/icon.svg',
      tag: d.tag || 'stock-alert',
      renotify: true,
    })
  );
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(cs => {
      if (cs.length > 0) { cs[0].focus(); return; }
      return clients.openWindow('/');
    })
  );
});
