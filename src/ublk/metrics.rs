use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct UblkMetrics {
    add_requests_total: AtomicU64,
    add_success_total: AtomicU64,
    add_failures_total: AtomicU64,
    remove_requests_total: AtomicU64,
    remove_success_total: AtomicU64,
    remove_failures_total: AtomicU64,
    active_devices: AtomicI64,
    drain_timeouts_total: AtomicU64,
    force_detach_total: AtomicU64,
}

impl UblkMetrics {
    pub fn record_add_result(&self, ok: bool) {
        self.add_requests_total.fetch_add(1, Ordering::Relaxed);
        if ok {
            self.add_success_total.fetch_add(1, Ordering::Relaxed);
            self.active_devices.fetch_add(1, Ordering::Relaxed);
        } else {
            self.add_failures_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_remove_result(&self, ok: bool, detached: bool) {
        self.remove_requests_total.fetch_add(1, Ordering::Relaxed);
        if ok {
            self.remove_success_total.fetch_add(1, Ordering::Relaxed);
            if detached {
                self.active_devices.fetch_sub(1, Ordering::Relaxed);
            }
        } else {
            self.remove_failures_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_drain_timeout(&self) {
        self.drain_timeouts_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_force_detach(&self) {
        self.force_detach_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn render_prometheus(&self) -> String {
        let active = self.active_devices.load(Ordering::Relaxed).max(0) as u64;
        format!(
            concat!(
                "# TYPE temporal_ublk_add_requests_total counter\n",
                "temporal_ublk_add_requests_total {}\n",
                "# TYPE temporal_ublk_add_success_total counter\n",
                "temporal_ublk_add_success_total {}\n",
                "# TYPE temporal_ublk_add_failures_total counter\n",
                "temporal_ublk_add_failures_total {}\n",
                "# TYPE temporal_ublk_remove_requests_total counter\n",
                "temporal_ublk_remove_requests_total {}\n",
                "# TYPE temporal_ublk_remove_success_total counter\n",
                "temporal_ublk_remove_success_total {}\n",
                "# TYPE temporal_ublk_remove_failures_total counter\n",
                "temporal_ublk_remove_failures_total {}\n",
                "# TYPE temporal_ublk_active_devices gauge\n",
                "temporal_ublk_active_devices {}\n",
                "# TYPE temporal_ublk_drain_timeouts_total counter\n",
                "temporal_ublk_drain_timeouts_total {}\n",
                "# TYPE temporal_ublk_force_detach_total counter\n",
                "temporal_ublk_force_detach_total {}\n",
            ),
            self.add_requests_total.load(Ordering::Relaxed),
            self.add_success_total.load(Ordering::Relaxed),
            self.add_failures_total.load(Ordering::Relaxed),
            self.remove_requests_total.load(Ordering::Relaxed),
            self.remove_success_total.load(Ordering::Relaxed),
            self.remove_failures_total.load(Ordering::Relaxed),
            active,
            self.drain_timeouts_total.load(Ordering::Relaxed),
            self.force_detach_total.load(Ordering::Relaxed),
        )
    }
}
