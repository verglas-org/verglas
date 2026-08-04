//! A cron matcher with Vixie-cron semantics — the source harness's cron
//! trigger.
//!
//! Five space-separated fields: minute (0-59), hour (0-23), day-of-month
//! (1-31), month (1-12 or JAN-DEC), day-of-week (0-6 or SUN-SAT, with 7 also
//! Sunday). Each field is `*`, a list of `,`-separated terms, and each term is a
//! single value, a `lo-hi` range, or either of those with a `/step`. `*/step`
//! means every `step` values across the whole range.
//!
//! # The day-of-month / day-of-week rule
//!
//! Vixie cron's one subtlety: when *both* day-of-month and day-of-week are
//! restricted (neither is `*`), a timestamp matches if *either* field matches.
//! When only one is restricted, only that one must match. This is a pure port of
//! the control-plane schedule semantics; the parity tests pin the standard cases
//! (`0 0 * * *`, `*/15 * * * *`, `0 9 * * MON-FRI`, `0 0 1 * *`, and the
//! dom-or-dow `0 0 13 * FRI`).

use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};

/// A parsed cron schedule: the allowed values of each field plus whether the two
/// day fields were restricted (for the OR rule).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronSchedule {
    minutes: Vec<u32>,
    hours: Vec<u32>,
    days_of_month: Vec<u32>,
    months: Vec<u32>,
    days_of_week: Vec<u32>,
    dom_restricted: bool,
    dow_restricted: bool,
}

/// A cron expression that could not be parsed.
#[derive(Debug, thiserror::Error)]
#[error("invalid cron expression: {0}")]
pub struct CronError(pub String);

/// Three-letter month names, index 0 = January.
const MONTHS: [&str; 12] = [
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
];

/// Three-letter day-of-week names, index 0 = Sunday.
const DOW: [&str; 7] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

impl CronSchedule {
    /// Parses a five-field cron expression. Extra whitespace between fields is
    /// tolerated; a wrong field count or an out-of-range term is an error.
    pub fn parse(expr: &str) -> Result<CronSchedule, CronError> {
        let fields: Vec<&str> = expr.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(CronError(format!(
                "expected 5 fields, got {}: `{expr}`",
                fields.len()
            )));
        }
        let minutes = parse_field(fields[0], 0, 59, None)?;
        let hours = parse_field(fields[1], 0, 23, None)?;
        let days_of_month = parse_field(fields[2], 1, 31, None)?;
        let months = parse_field(fields[3], 1, 12, Some(&MONTHS))?;
        // Day-of-week: names index 0 = Sunday; 7 normalises to 0.
        let mut days_of_week = parse_field(&fields[4].replace('7', "0"), 0, 6, Some(&DOW))?;
        days_of_week.sort_unstable();
        days_of_week.dedup();

        Ok(CronSchedule {
            minutes,
            hours,
            days_of_month,
            months,
            days_of_week,
            dom_restricted: fields[2] != "*",
            dow_restricted: fields[4] != "*",
        })
    }

    /// Whether `at` (a UTC instant, truncated to the minute) satisfies the
    /// schedule.
    pub fn matches(&self, at: &DateTime<Utc>) -> bool {
        if !self.minutes.contains(&at.minute())
            || !self.hours.contains(&at.hour())
            || !self.months.contains(&at.month())
        {
            return false;
        }
        // chrono weekday: Mon=0..Sun=6 via num_days_from_monday; cron uses
        // Sun=0..Sat=6. Convert.
        let dow = at.weekday().num_days_from_sunday();
        let dom_ok = self.days_of_month.contains(&at.day());
        let dow_ok = self.days_of_week.contains(&dow);
        if self.dom_restricted && self.dow_restricted {
            dom_ok || dow_ok
        } else {
            dom_ok && dow_ok
        }
    }

    /// The first minute strictly after `from` that matches, searching forward.
    /// Bounded to four years so a schedule that can never fire (e.g. Feb 30)
    /// returns `None` rather than looping.
    pub fn next_after(&self, from: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        // Start at the next whole minute after `from`.
        let mut cursor = Utc
            .timestamp_opt(from.timestamp() - from.timestamp().rem_euclid(60) + 60, 0)
            .single()?;
        // Four years of minutes is a generous upper bound covering every
        // reachable schedule (leap years included).
        let limit = 4 * 366 * 24 * 60;
        for _ in 0..limit {
            if self.matches(&cursor) {
                return Some(cursor);
            }
            cursor += Duration::minutes(1);
        }
        None
    }
}

/// Parses one field into its sorted, deduplicated set of allowed values. `names`
/// maps three-letter aliases to zero-based indices (offset by `min`).
fn parse_field(
    field: &str,
    min: u32,
    max: u32,
    names: Option<&[&str]>,
) -> Result<Vec<u32>, CronError> {
    let mut out = Vec::new();
    for term in field.split(',') {
        let (range_part, step) = match term.split_once('/') {
            Some((r, s)) => {
                let step: u32 = s
                    .parse()
                    .map_err(|_| CronError(format!("bad step `{s}` in `{field}`")))?;
                if step == 0 {
                    return Err(CronError(format!("zero step in `{field}`")));
                }
                (r, step)
            }
            None => (term, 1),
        };
        let (lo, hi) = if range_part == "*" {
            (min, max)
        } else if let Some((a, b)) = range_part.split_once('-') {
            (
                parse_value(a, min, max, names, field)?,
                parse_value(b, min, max, names, field)?,
            )
        } else {
            let v = parse_value(range_part, min, max, names, field)?;
            // A bare value with a step ranges from the value up to the max.
            if step > 1 { (v, max) } else { (v, v) }
        };
        if lo > hi {
            return Err(CronError(format!(
                "range {lo}-{hi} is inverted in `{field}`"
            )));
        }
        let mut v = lo;
        while v <= hi {
            out.push(v);
            v += step;
        }
    }
    out.sort_unstable();
    out.dedup();
    if out.is_empty() {
        return Err(CronError(format!("field `{field}` matched no values")));
    }
    Ok(out)
}

/// Parses a single value: a number in `[min, max]`, or a three-letter name.
fn parse_value(
    token: &str,
    min: u32,
    max: u32,
    names: Option<&[&str]>,
    field: &str,
) -> Result<u32, CronError> {
    let token = token.trim();
    if let Some(names) = names
        && let Some(idx) = names.iter().position(|n| n.eq_ignore_ascii_case(token))
    {
        // Names are zero-based; the field's own `min` offsets them (months
        // start at 1, days-of-week at 0).
        return Ok(idx as u32 + min);
    }
    let value: u32 = token
        .parse()
        .map_err(|_| CronError(format!("bad value `{token}` in `{field}`")))?;
    if value < min || value > max {
        return Err(CronError(format!(
            "value {value} out of range {min}-{max} in `{field}`"
        )));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0)
            .single()
            .expect("valid")
    }

    /// `0 0 * * *` fires only at midnight.
    #[test]
    fn daily_midnight() {
        let c = CronSchedule::parse("0 0 * * *").expect("parse");
        assert!(c.matches(&at(2026, 7, 24, 0, 0)));
        assert!(!c.matches(&at(2026, 7, 24, 0, 1)));
        assert!(!c.matches(&at(2026, 7, 24, 1, 0)));
    }

    /// `*/15 * * * *` fires at minutes 0, 15, 30, 45.
    #[test]
    fn every_fifteen_minutes() {
        let c = CronSchedule::parse("*/15 * * * *").expect("parse");
        for m in [0, 15, 30, 45] {
            assert!(c.matches(&at(2026, 7, 24, 9, m)), "minute {m}");
        }
        for m in [1, 14, 16, 44] {
            assert!(!c.matches(&at(2026, 7, 24, 9, m)), "minute {m}");
        }
    }

    /// `0 9 * * MON-FRI` fires at 09:00 on weekdays, not weekends.
    #[test]
    fn weekday_names() {
        let c = CronSchedule::parse("0 9 * * MON-FRI").expect("parse");
        // 2026-07-24 is a Friday; 2026-07-25 a Saturday.
        assert!(c.matches(&at(2026, 7, 24, 9, 0)), "Friday");
        assert!(!c.matches(&at(2026, 7, 25, 9, 0)), "Saturday");
        assert!(!c.matches(&at(2026, 7, 24, 10, 0)), "wrong hour");
    }

    /// `0 0 1 * *` fires on the first of every month.
    #[test]
    fn monthly_first() {
        let c = CronSchedule::parse("0 0 1 * *").expect("parse");
        assert!(c.matches(&at(2026, 8, 1, 0, 0)));
        assert!(!c.matches(&at(2026, 8, 2, 0, 0)));
    }

    /// The Vixie OR rule: `0 0 13 * FRI` fires on the 13th OR any Friday.
    #[test]
    fn dom_or_dow_when_both_restricted() {
        let c = CronSchedule::parse("0 0 13 * FRI").expect("parse");
        // 2026-07-13 is a Monday: matches by day-of-month.
        assert!(c.matches(&at(2026, 7, 13, 0, 0)), "the 13th");
        // 2026-07-24 is a Friday: matches by day-of-week.
        assert!(c.matches(&at(2026, 7, 24, 0, 0)), "a Friday");
        // 2026-07-14 is a Tuesday, not the 13th: no match.
        assert!(!c.matches(&at(2026, 7, 14, 0, 0)), "neither");
    }

    /// Month names and numeric ranges parse equivalently.
    #[test]
    fn month_names_and_numbers() {
        let named = CronSchedule::parse("0 0 1 JAN,JUL *").expect("parse");
        let numeric = CronSchedule::parse("0 0 1 1,7 *").expect("parse");
        assert_eq!(named.months, numeric.months);
        assert_eq!(named.months, vec![1, 7]);
    }

    /// `next_after` returns the next matching minute.
    #[test]
    fn next_after_advances() {
        let c = CronSchedule::parse("30 2 * * *").expect("parse");
        let next = c.next_after(&at(2026, 7, 24, 2, 0)).expect("next");
        assert_eq!(next, at(2026, 7, 24, 2, 30));
        let after = c.next_after(&at(2026, 7, 24, 2, 30)).expect("next");
        assert_eq!(after, at(2026, 7, 25, 2, 30), "rolls to the next day");
    }

    /// Malformed expressions are rejected, not silently accepted.
    #[test]
    fn rejects_malformed() {
        assert!(CronSchedule::parse("* * * *").is_err(), "too few fields");
        assert!(CronSchedule::parse("60 * * * *").is_err(), "minute > 59");
        assert!(CronSchedule::parse("* * * 13 *").is_err(), "month > 12");
        assert!(CronSchedule::parse("*/0 * * * *").is_err(), "zero step");
    }
}
