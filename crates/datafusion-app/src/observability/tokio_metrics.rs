use log::info;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio_metrics::RuntimeMonitor;

pub struct TokioMetricsCollector {
    _reporter_handle: tokio::task::JoinHandle<()>,
}

impl TokioMetricsCollector {
    /// Start collecting metrics from the given runtime handle
    ///
    /// # Arguments
    /// * `handle` - Tokio runtime handle to monitor
    /// * `runtime_name` - Name prefix for metrics (e.g., "io_runtime" or "cpu_runtime")
    /// * `interval` - How often to poll and report metrics
    pub fn start(handle: Handle, runtime_name: String, interval: Duration) -> Self {
        info!(
            "Starting tokio metrics collection for {} with interval {:?}",
            runtime_name, interval
        );

        let runtime_monitor = RuntimeMonitor::new(&handle);

        let reporter_handle = tokio::spawn(async move {
            let mut intervals = runtime_monitor.intervals();
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval_timer.tick().await;

                if let Some(metrics) = intervals.next() {
                    // Report metrics using metrics crate
                    use metrics::{counter, gauge};

                    let prefix = &runtime_name;

                    // Always available metrics
                    gauge!(format!("{}_workers_count", prefix)).set(metrics.workers_count as f64);
                    counter!(format!("{}_total_park_count", prefix))
                        .absolute(metrics.total_park_count);
                    gauge!(format!("{}_max_park_count", prefix)).set(metrics.max_park_count as f64);
                    gauge!(format!("{}_min_park_count", prefix)).set(metrics.min_park_count as f64);
                    gauge!(format!("{}_total_busy_duration_us", prefix))
                        .set(metrics.total_busy_duration.as_micros() as f64);
                    gauge!(format!("{}_max_busy_duration_us", prefix))
                        .set(metrics.max_busy_duration.as_micros() as f64);
                    gauge!(format!("{}_min_busy_duration_us", prefix))
                        .set(metrics.min_busy_duration.as_micros() as f64);
                    gauge!(format!("{}_global_queue_depth", prefix))
                        .set(metrics.global_queue_depth as f64);
                    gauge!(format!("{}_elapsed_seconds", prefix))
                        .set(metrics.elapsed.as_secs_f64());
                }
            }
        });

        Self {
            _reporter_handle: reporter_handle,
        }
    }

    /// Start monitoring the current runtime
    pub fn start_current(runtime_name: String, interval: Duration) -> Self {
        let handle = Handle::current();
        Self::start(handle, runtime_name, interval)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_collector_starts() {
        let _collector = TokioMetricsCollector::start_current(
            "test_runtime".to_string(),
            Duration::from_secs(1),
        );

        // Sleep to allow at least one metrics report
        tokio::time::sleep(Duration::from_millis(1100)).await;

        // Collector should still be alive
        // In a real test, we'd verify metrics are being recorded
    }
}
