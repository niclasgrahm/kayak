//! Descriptive statistics over a slice of numbers, and nothing else.
//!
//! This is the numeric half every statistical feature shares: the `reduce`
//! transform's `avg`/`median`/`stddev`/`slope`, the script builtins (`mean`,
//! `linfit`, `ewma` …), and the streaming transforms still to come (`rolling`,
//! `smooth`, `detect`) are each a way of choosing *which* numbers reach one of
//! these functions. Keeping the arithmetic here, with no `Value`, no `Dynamic`
//! and no config in sight, is what lets one test per function stand for all of
//! its callers.
//!
//! Two rules hold throughout:
//!
//! - **Nothing here returns a NaN on purpose.** A function that is undefined
//!   for its input — the mean of nothing, the slope of one point, a z-score
//!   with no spread — returns `None`, and each caller decides what that is in
//!   its own vocabulary (`null` in a reduced message, `()` in a script). A NaN
//!   would travel silently through every comparison downstream and turn into a
//!   `null` only at the output, far from where it was made.
//! - **Spread is the *population* kind.** A window holds every message that
//!   arrived in it, so it is the population, not a sample of one. `variance`
//!   divides by *n*, `stddev` is its root, and `skew`/`kurtosis` are the
//!   population moments. This is the rule `reduce`'s `stddev` has always
//!   followed and every function here follows it.

/// The straight line through a set of points, by least squares.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Fit {
    /// How much `y` changes per unit of `x`.
    pub slope: f64,
    /// `y` where `x` is zero.
    pub intercept: f64,
    /// How much of the variation in `y` the line explains, 0 to 1. `1.0` when
    /// the points are exactly collinear, including when every `y` is the same.
    pub r2: f64,
}

/// A histogram: `bins` equal-width buckets between the smallest and largest
/// value, with `edges` one longer than `counts`.
#[derive(Clone, Debug, PartialEq)]
pub struct Histogram {
    pub edges: Vec<f64>,
    pub counts: Vec<usize>,
}

/// A length as a float. A batch would have to hold 2^53 numbers for this to
/// lose anything, and it is all in memory at once.
#[allow(clippy::cast_precision_loss)]
fn n(numbers: &[f64]) -> f64 {
    numbers.len() as f64
}

#[must_use]
pub fn sum(numbers: &[f64]) -> f64 {
    numbers.iter().sum()
}

/// The arithmetic mean, or `None` of nothing.
#[must_use]
pub fn mean(numbers: &[f64]) -> Option<f64> {
    if numbers.is_empty() {
        return None;
    }
    Some(sum(numbers) / n(numbers))
}

/// The smallest value. NaNs sort with `total_cmp`, so one that arrived as a
/// number has a defined place rather than poisoning the comparison.
#[must_use]
pub fn min(numbers: &[f64]) -> Option<f64> {
    numbers.iter().copied().min_by(f64::total_cmp)
}

/// The largest value, compared as `min` compares.
#[must_use]
pub fn max(numbers: &[f64]) -> Option<f64> {
    numbers.iter().copied().max_by(f64::total_cmp)
}

/// The middle value, or the mean of the two middle ones.
#[must_use]
pub fn median(numbers: &[f64]) -> Option<f64> {
    quantile(numbers, 0.5)
}

/// The value a fraction `q` of the way through the sorted numbers, linearly
/// interpolated between the two it falls between (type 7, which is what R,
/// numpy and a spreadsheet all default to). `q` outside `0..=1` is `None`.
#[must_use]
pub fn quantile(numbers: &[f64], q: f64) -> Option<f64> {
    if numbers.is_empty() || !(0.0..=1.0).contains(&q) {
        return None;
    }
    let mut sorted = numbers.to_vec();
    sorted.sort_by(f64::total_cmp);
    let position = q * (n(&sorted) - 1.0);
    let below = position.floor();
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "0 <= below <= len - 1 by construction"
    )]
    let index = below as usize;
    let fraction = position - below;
    let lower = sorted[index];
    let upper = sorted.get(index + 1).copied().unwrap_or(lower);
    Some(lower + (upper - lower) * fraction)
}

/// The population variance.
#[must_use]
pub fn variance(numbers: &[f64]) -> Option<f64> {
    let centre = mean(numbers)?;
    let squares: Vec<f64> = numbers.iter().map(|x| (x - centre).powi(2)).collect();
    mean(&squares)
}

/// The population standard deviation.
#[must_use]
pub fn stddev(numbers: &[f64]) -> Option<f64> {
    variance(numbers).map(f64::sqrt)
}

/// The median absolute deviation from the median — a spread that one wild
/// reading cannot move, which is what makes it the right scale for a robust
/// detector. Raw, not scaled by 1.4826; a caller wanting a σ-equivalent
/// multiplies.
#[must_use]
pub fn mad(numbers: &[f64]) -> Option<f64> {
    let centre = median(numbers)?;
    let deviations: Vec<f64> = numbers.iter().map(|x| (x - centre).abs()).collect();
    median(&deviations)
}

/// Every value as its distance from the mean in standard deviations.
///
/// A set with no spread has no z-scores — every value *is* the mean, and
/// dividing by zero would make them all NaN — so that is `None` rather than a
/// list of zeroes that a threshold would then read as "nothing unusual".
#[must_use]
pub fn zscore(numbers: &[f64]) -> Option<Vec<f64>> {
    let centre = mean(numbers)?;
    let spread = stddev(numbers)?;
    if spread == 0.0 {
        return None;
    }
    Some(numbers.iter().map(|x| (x - centre) / spread).collect())
}

/// The third standardised moment: positive when the long tail is to the right.
/// `None` for fewer than two values or no spread.
#[must_use]
pub fn skew(numbers: &[f64]) -> Option<f64> {
    standardised_moment(numbers, 3)
}

/// The *excess* kurtosis — the fourth standardised moment less 3, so a normal
/// distribution reads as `0` and a heavy-tailed one as positive.
#[must_use]
pub fn kurtosis(numbers: &[f64]) -> Option<f64> {
    standardised_moment(numbers, 4).map(|k| k - 3.0)
}

fn standardised_moment(numbers: &[f64], order: i32) -> Option<f64> {
    if numbers.len() < 2 {
        return None;
    }
    let centre = mean(numbers)?;
    let spread = stddev(numbers)?;
    if spread == 0.0 {
        return None;
    }
    let powers: Vec<f64> = numbers
        .iter()
        .map(|x| ((x - centre) / spread).powi(order))
        .collect();
    mean(&powers)
}

/// The root mean square — the magnitude of a signal that swings either side of
/// zero, where a mean would cancel it out.
#[must_use]
pub fn rms(numbers: &[f64]) -> Option<f64> {
    let squares: Vec<f64> = numbers.iter().map(|x| x * x).collect();
    mean(&squares).map(f64::sqrt)
}

/// Each value less the one before it; one shorter than the input.
#[must_use]
pub fn diff(numbers: &[f64]) -> Vec<f64> {
    numbers.windows(2).map(|pair| pair[1] - pair[0]).collect()
}

/// The running total; the same length as the input.
#[must_use]
pub fn cumsum(numbers: &[f64]) -> Vec<f64> {
    let mut total = 0.0;
    numbers
        .iter()
        .map(|x| {
            total += x;
            total
        })
        .collect()
}

/// An exponentially weighted moving average, seeded with the first value:
/// each output is `alpha * x + (1 - alpha) * previous`. `alpha` outside
/// `0..=1` is `None` — it is a weight, and a weight past one is a divergent
/// series rather than a smoother one.
#[must_use]
pub fn ewma(numbers: &[f64], alpha: f64) -> Option<Vec<f64>> {
    if !(0.0..=1.0).contains(&alpha) {
        return None;
    }
    let mut out = Vec::with_capacity(numbers.len());
    let mut previous: Option<f64> = None;
    for &x in numbers {
        let next = match previous {
            None => x,
            Some(p) => alpha * x + (1.0 - alpha) * p,
        };
        out.push(next);
        previous = Some(next);
    }
    Some(out)
}

/// The `alpha` that gives an [`ewma`] a half-life of `half_life` steps — the
/// number of steps after which a value's weight has halved. The spelling
/// somebody with a sensor has an intuition for, where `alpha` is not one.
#[must_use]
pub fn alpha_for_half_life(half_life: f64) -> Option<f64> {
    if half_life <= 0.0 || !half_life.is_finite() {
        return None;
    }
    Some(1.0 - 0.5_f64.powf(1.0 / half_life))
}

/// The least-squares line through `(x, y)` pairs.
///
/// `None` for fewer than two points, mismatched lengths, or every `x` the
/// same — a vertical line has no slope to report.
#[must_use]
pub fn linfit(xs: &[f64], ys: &[f64]) -> Option<Fit> {
    if xs.len() != ys.len() || xs.len() < 2 {
        return None;
    }
    let x_mean = mean(xs)?;
    let y_mean = mean(ys)?;
    let mut sxx = 0.0;
    let mut sxy = 0.0;
    let mut syy = 0.0;
    for (x, y) in xs.iter().zip(ys) {
        let dx = x - x_mean;
        let dy = y - y_mean;
        sxx += dx * dx;
        sxy += dx * dy;
        syy += dy * dy;
    }
    if sxx == 0.0 {
        return None;
    }
    let slope = sxy / sxx;
    let intercept = y_mean - slope * x_mean;
    // A flat series is fitted exactly by a flat line: the line explains all
    // of no variation. Without this arm it would be 0/0.
    let r2 = if syy == 0.0 { 1.0 } else { (sxy * sxy) / (sxx * syy) };
    Some(Fit {
        slope,
        intercept,
        r2,
    })
}

/// [`linfit`] against position — `x` is `0, 1, 2 …` — for a series with no
/// time of its own, where the slope is "per message".
#[must_use]
pub fn linfit_indexed(ys: &[f64]) -> Option<Fit> {
    #[allow(clippy::cast_precision_loss)]
    let xs: Vec<f64> = (0..ys.len()).map(|i| i as f64).collect();
    linfit(&xs, ys)
}

/// The least-squares polynomial of `order` through `(x, y)` pairs, as
/// coefficients lowest power first. `None` for fewer points than
/// coefficients, mismatched lengths, or a system too degenerate to solve.
///
/// Solved by the normal equations with partial pivoting, which is fine for
/// the low orders a smoother uses — a window of a few dozen points and a
/// cubic. It is not a general polynomial fitter and does not want to be.
#[must_use]
pub fn polyfit(xs: &[f64], ys: &[f64], order: usize) -> Option<Vec<f64>> {
    let terms = order + 1;
    if xs.len() != ys.len() || xs.len() < terms {
        return None;
    }
    // Build the normal equations A·c = b with A[i][j] = Σ x^(i+j), b[i] = Σ x^i·y.
    let mut a = vec![vec![0.0; terms]; terms];
    let mut b = vec![0.0; terms];
    for (&x, &y) in xs.iter().zip(ys) {
        let mut powers = vec![1.0; 2 * terms - 1];
        for i in 1..powers.len() {
            powers[i] = powers[i - 1] * x;
        }
        for i in 0..terms {
            for j in 0..terms {
                a[i][j] += powers[i + j];
            }
            b[i] += powers[i] * y;
        }
    }
    // Gaussian elimination with partial pivoting.
    for column in 0..terms {
        let pivot = (column..terms).max_by(|&p, &q| a[p][column].abs().total_cmp(&a[q][column].abs()))?;
        if a[pivot][column].abs() < 1e-12 {
            return None;
        }
        a.swap(column, pivot);
        b.swap(column, pivot);
        let (pivot_row, below) = a.split_at_mut(column + 1);
        let pivot_row = &pivot_row[column];
        for (offset, row) in below.iter_mut().enumerate() {
            let factor = row[column] / pivot_row[column];
            for (cell, &above) in row.iter_mut().zip(pivot_row).skip(column) {
                *cell -= factor * above;
            }
            b[column + 1 + offset] -= factor * b[column];
        }
    }
    let mut coefficients = vec![0.0; terms];
    for row in (0..terms).rev() {
        let tail: f64 = (row + 1..terms).map(|k| a[row][k] * coefficients[k]).sum();
        coefficients[row] = (b[row] - tail) / a[row][row];
    }
    Some(coefficients)
}

/// A trailing Savitzky–Golay smoother's answer for the newest of `ys`: the
/// polynomial of `order` fitted to the window (taken as evenly spaced),
/// evaluated at its last point. `None` when the window is too short for the
/// order.
///
/// The points are placed at `x = -(n-1) … 0`, so the answer is the fitted
/// polynomial's constant term and nothing has to be evaluated.
#[must_use]
pub fn savitzky_golay_last(ys: &[f64], order: usize) -> Option<f64> {
    #[allow(clippy::cast_precision_loss)]
    let xs: Vec<f64> = (0..ys.len()).map(|i| i as f64 - (ys.len() as f64 - 1.0)).collect();
    polyfit(&xs, ys, order).and_then(|c| c.first().copied())
}

/// The autocorrelation at `lag` — how much a value resembles the one `lag`
/// steps before it, from −1 to 1. `None` when the lag leaves fewer than two
/// pairs or the series has no spread.
#[must_use]
pub fn autocorr(numbers: &[f64], lag: usize) -> Option<f64> {
    if lag == 0 || numbers.len() < lag + 2 {
        return None;
    }
    let centre = mean(numbers)?;
    let total: f64 = numbers.iter().map(|x| (x - centre).powi(2)).sum();
    if total == 0.0 {
        return None;
    }
    let lagged: f64 = numbers[lag..]
        .iter()
        .zip(&numbers[..numbers.len() - lag])
        .map(|(a, b)| (a - centre) * (b - centre))
        .sum();
    Some(lagged / total)
}

/// The positions of the local maxima — every value strictly greater than both
/// of its neighbours. The two ends can't be peaks, having only one neighbour,
/// and a plateau isn't one either: "strictly" is what keeps a flat top from
/// counting as one peak per sample.
#[must_use]
pub fn peaks(numbers: &[f64]) -> Vec<usize> {
    numbers
        .windows(3)
        .enumerate()
        .filter(|(_, w)| w[1] > w[0] && w[1] > w[2])
        .map(|(i, _)| i + 1)
        .collect()
}

/// `bins` equal-width buckets from the smallest to the largest value, the
/// largest landing in the last bucket rather than in one past it. `None` of
/// nothing or of zero bins; a single distinct value goes in the first bucket.
#[must_use]
pub fn histogram(numbers: &[f64], bins: usize) -> Option<Histogram> {
    if bins == 0 {
        return None;
    }
    let low = min(numbers)?;
    let high = max(numbers)?;
    #[allow(clippy::cast_precision_loss)]
    let width = (high - low) / bins as f64;
    let edges: Vec<f64> = (0..=bins)
        .map(|i| {
            #[allow(clippy::cast_precision_loss)]
            let step = i as f64;
            low + width * step
        })
        .collect();
    let mut counts = vec![0; bins];
    for &x in numbers {
        let bucket = if width == 0.0 {
            0
        } else {
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "x lies within [low, high], so the quotient is within [0, bins]"
            )]
            let raw = ((x - low) / width).floor() as usize;
            raw.min(bins - 1)
        };
        counts[bucket] += 1;
    }
    Some(Histogram { edges, counts })
}

/// `x` held within `low..=high`.
#[must_use]
pub fn clamp(x: f64, low: f64, high: f64) -> f64 {
    if low > high {
        return x;
    }
    x.max(low).min(high)
}

/// `y` at `x` by straight lines between the known `(xs, ys)` points, which
/// must be sorted by `x`. Outside the range it holds the nearest end's value
/// rather than extrapolating. `None` when there are no points or the lengths
/// differ.
#[must_use]
pub fn interp(xs: &[f64], ys: &[f64], x: f64) -> Option<f64> {
    if xs.is_empty() || xs.len() != ys.len() {
        return None;
    }
    if x <= xs[0] {
        return Some(ys[0]);
    }
    if x >= xs[xs.len() - 1] {
        return Some(ys[ys.len() - 1]);
    }
    let right = xs.partition_point(|&known| known < x);
    let (x0, x1) = (xs[right - 1], xs[right]);
    let (y0, y1) = (ys[right - 1], ys[right]);
    #[allow(clippy::float_cmp, reason = "an exact repeat of an x is what makes the division undefined")]
    if x1 == x0 {
        return Some(y0);
    }
    Some(y0 + (y1 - y0) * (x - x0) / (x1 - x0))
}

/// The dynamic-time-warping distance between two series: the sum of absolute
/// differences along the alignment that makes them most alike, so two cycles
/// of the same shape at different speeds are close. O(n·m) time and O(m)
/// memory. `None` when either is empty.
#[must_use]
pub fn dtw(a: &[f64], b: &[f64]) -> Option<f64> {
    if a.is_empty() || b.is_empty() {
        return None;
    }
    let mut previous = vec![f64::INFINITY; b.len() + 1];
    previous[0] = 0.0;
    let mut current = vec![f64::INFINITY; b.len() + 1];
    for &x in a {
        current[0] = f64::INFINITY;
        for (j, &y) in b.iter().enumerate() {
            let cost = (x - y).abs();
            let best = previous[j].min(previous[j + 1]).min(current[j]);
            current[j + 1] = cost + best;
        }
        std::mem::swap(&mut previous, &mut current);
    }
    Some(previous[b.len()])
}

#[cfg(test)]
#[allow(clippy::float_cmp, reason = "the expected values are exact in binary")]
mod tests {
    use super::*;

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn the_centre_of_a_series() {
        let xs = [1.0, 2.0, 3.0, 4.0];
        assert_eq!(sum(&xs), 10.0);
        assert_eq!(mean(&xs), Some(2.5));
        assert_eq!(median(&xs), Some(2.5));
        assert_eq!(median(&[3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(min(&xs), Some(1.0));
        assert_eq!(max(&xs), Some(4.0));
    }

    /// The rule the module is built around: undefined is `None`, never NaN.
    #[test]
    fn nothing_here_produces_a_nan() {
        assert_eq!(mean(&[]), None);
        assert_eq!(median(&[]), None);
        assert_eq!(stddev(&[]), None);
        assert_eq!(mad(&[]), None);
        assert_eq!(zscore(&[]), None);
        assert_eq!(zscore(&[2.0, 2.0]), None);
        assert_eq!(skew(&[1.0]), None);
        assert_eq!(skew(&[1.0, 1.0, 1.0]), None);
        assert_eq!(kurtosis(&[1.0]), None);
        assert_eq!(rms(&[]), None);
        assert_eq!(linfit(&[1.0], &[1.0]), None);
        assert_eq!(linfit(&[1.0, 1.0], &[1.0, 2.0]), None, "a vertical line");
        assert_eq!(autocorr(&[1.0, 2.0], 1), None);
        assert_eq!(autocorr(&[1.0, 1.0, 1.0, 1.0], 1), None);
        assert_eq!(histogram(&[], 3), None);
        assert_eq!(histogram(&[1.0], 0), None);
        assert_eq!(interp(&[], &[], 1.0), None);
        assert_eq!(dtw(&[], &[1.0]), None);
        assert_eq!(ewma(&[1.0], 1.5), None);
        assert_eq!(quantile(&[1.0], 1.5), None);
    }

    #[test]
    fn quantiles_interpolate_between_neighbours() {
        let xs = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(quantile(&xs, 0.0), Some(1.0));
        assert_eq!(quantile(&xs, 1.0), Some(5.0));
        assert_eq!(quantile(&xs, 0.25), Some(2.0));
        assert_eq!(quantile(&[1.0, 2.0], 0.75), Some(1.75));
    }

    #[test]
    fn spread_is_the_population_kind() {
        let xs = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        assert_eq!(variance(&xs), Some(4.0));
        assert_eq!(stddev(&xs), Some(2.0));
        assert_eq!(mad(&[1.0, 2.0, 3.0, 4.0, 100.0]), Some(1.0));
        let z = zscore(&xs).unwrap_or_default();
        assert!(close(z[0], -1.5) && close(z[7], 2.0));
        assert!(close(rms(&[3.0, -4.0]).unwrap_or_default(), 12.5_f64.sqrt()));
    }

    #[test]
    fn shape_of_a_distribution() {
        let symmetric = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert!(close(skew(&symmetric).unwrap_or(1.0), 0.0));
        assert!(skew(&[1.0, 1.0, 1.0, 10.0]).unwrap_or_default() > 0.0);
        // a uniform set is lighter-tailed than normal
        assert!(kurtosis(&symmetric).unwrap_or_default() < 0.0);
    }

    #[test]
    fn running_series() {
        assert_eq!(diff(&[1.0, 4.0, 9.0]), vec![3.0, 5.0]);
        assert_eq!(diff(&[1.0]), Vec::<f64>::new());
        assert_eq!(cumsum(&[1.0, 2.0, 3.0]), vec![1.0, 3.0, 6.0]);
        assert_eq!(ewma(&[1.0, 3.0, 3.0], 0.5), Some(vec![1.0, 2.0, 2.5]));
        assert_eq!(ewma(&[], 0.5), Some(Vec::new()));
        let alpha = alpha_for_half_life(1.0).unwrap_or_default();
        assert!(close(alpha, 0.5));
        assert_eq!(alpha_for_half_life(0.0), None);
    }

    #[test]
    fn a_line_through_points() {
        let fit = linfit(&[0.0, 1.0, 2.0, 3.0], &[1.0, 3.0, 5.0, 7.0]).unwrap_or(Fit {
            slope: 0.0,
            intercept: 0.0,
            r2: 0.0,
        });
        assert!(close(fit.slope, 2.0) && close(fit.intercept, 1.0) && close(fit.r2, 1.0));

        let flat = linfit_indexed(&[4.0, 4.0, 4.0]).map(|f| (f.slope, f.r2));
        assert_eq!(flat, Some((0.0, 1.0)), "a flat line is fitted exactly");

        let noisy = linfit_indexed(&[1.0, 3.0, 2.0, 4.0]).map(|f| f.r2);
        assert!(noisy.is_some_and(|r2| r2 > 0.0 && r2 < 1.0));
    }

    #[test]
    fn a_polynomial_fit_recovers_its_coefficients() {
        let xs: Vec<f64> = (0..8).map(f64::from).collect();
        let ys: Vec<f64> = xs.iter().map(|x| 1.0 + 2.0 * x - 0.5 * x * x).collect();
        let c = polyfit(&xs, &ys, 2).unwrap_or_default();
        assert!(close(c[0], 1.0) && close(c[1], 2.0) && close(c[2], -0.5), "{c:?}");
        assert_eq!(polyfit(&xs[..2], &ys[..2], 2), None, "too few points");
        assert_eq!(polyfit(&[1.0, 1.0, 1.0], &[1.0, 2.0, 3.0], 1), None, "a vertical line");
    }

    #[test]
    fn savitzky_golay_keeps_a_parabola_and_smooths_noise() {
        let exact: Vec<f64> = (0..7).map(|i| f64::from(i * i)).collect();
        assert!(close(savitzky_golay_last(&exact, 2).unwrap_or_default(), 36.0));
        let noisy = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 5.0];
        let smoothed = savitzky_golay_last(&noisy, 2).unwrap_or_default();
        assert!(smoothed > 1.0 && smoothed < 5.0, "{smoothed}");
        assert_eq!(savitzky_golay_last(&[1.0, 2.0], 2), None);
    }

    #[test]
    fn autocorrelation_finds_a_period() {
        let wave = [1.0, -1.0, 1.0, -1.0, 1.0, -1.0];
        assert!(autocorr(&wave, 1).unwrap_or_default() < -0.5);
        assert!(autocorr(&wave, 2).unwrap_or_default() > 0.5);
        assert_eq!(autocorr(&wave, 0), None);
    }

    #[test]
    fn peaks_are_strict_local_maxima() {
        assert_eq!(peaks(&[0.0, 2.0, 0.0, 3.0, 3.0, 0.0, 5.0]), vec![1]);
        assert_eq!(peaks(&[1.0, 2.0]), Vec::<usize>::new());
    }

    #[test]
    fn a_histogram_puts_the_top_value_in_the_last_bucket() {
        let h = histogram(&[0.0, 1.0, 2.0, 3.0, 4.0], 2).unwrap_or(Histogram {
            edges: vec![],
            counts: vec![],
        });
        assert_eq!(h.edges, vec![0.0, 2.0, 4.0]);
        assert_eq!(h.counts, vec![2, 3]);

        let flat = histogram(&[7.0, 7.0], 3).map(|h| h.counts);
        assert_eq!(flat, Some(vec![2, 0, 0]));
    }

    #[test]
    fn interpolation_holds_at_the_ends() {
        let xs = [0.0, 10.0, 20.0];
        let ys = [0.0, 100.0, 0.0];
        assert_eq!(interp(&xs, &ys, 5.0), Some(50.0));
        assert_eq!(interp(&xs, &ys, 15.0), Some(50.0));
        assert_eq!(interp(&xs, &ys, -5.0), Some(0.0));
        assert_eq!(interp(&xs, &ys, 25.0), Some(0.0));
        assert_eq!(interp(&xs, &ys, 10.0), Some(100.0));
    }

    #[test]
    fn clamping() {
        assert_eq!(clamp(5.0, 0.0, 3.0), 3.0);
        assert_eq!(clamp(-1.0, 0.0, 3.0), 0.0);
        assert_eq!(clamp(1.0, 0.0, 3.0), 1.0);
    }

    #[test]
    fn dtw_is_zero_for_the_same_shape_at_another_speed() {
        let fast = [0.0, 1.0, 2.0, 1.0, 0.0];
        let slow = [0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 1.0, 1.0, 0.0, 0.0];
        assert_eq!(dtw(&fast, &slow), Some(0.0));
        assert_eq!(dtw(&[0.0, 0.0], &[1.0, 1.0]), Some(2.0));
    }
}
