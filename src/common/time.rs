use chrono::Utc;

pub fn get_current_time_f64() -> f64 {
    let now = Utc::now();
    now.timestamp() as f64 + f64::from(now.timestamp_subsec_nanos()) * 1e-9
}
