use std::ops::RangeInclusive;

/// Split a list of addresses into ranges that are cheap to read out.
///
/// The ranges that cover all of the addresses are picked such that there are as few of them as
/// possible and then such that they cover as few addresses as possible. No range is longer than
/// `max_range_len`. The `addresses` must be sorted and without duplicates.
pub(crate) fn find_optimal_ranges(
    addresses: &[u16],
    max_range_len: u16,
) -> Vec<RangeInclusive<u16>> {
    debug_assert!(addresses.windows(2).all(|w| w[0] < w[1]));
    let n = addresses.len();
    // `best[i]` is the cost (number of ranges, total length) for covering `addresses[i..]`
    let mut best: Vec<(u32, u32)> = vec![(0, 0); n + 1];
    let mut choices: Vec<usize> = vec![0; n];
    for i in (0..n).rev() {
        let mut best_cost = (u32::MAX, u32::MAX);
        for j in i..n {
            let len = u32::from(addresses[j] - addresses[i]) + 1;
            if len > u32::from(max_range_len) && j > i {
                break;
            }
            let cost = (1 + best[j + 1].0, len + best[j + 1].1);
            if cost < best_cost {
                best_cost = cost;
                choices[i] = j;
            }
        }
        best[i] = best_cost;
    }
    let mut ranges = Vec::new();
    let mut i = 0;
    while i < n {
        let j = choices[i];
        ranges.push(addresses[i]..=addresses[j]);
        i = j + 1;
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::find_optimal_ranges as plan;

    #[test]
    fn empty() {
        assert!(plan(&[], 10).is_empty());
    }

    #[test]
    fn merges_close_addresses() {
        assert_eq!(plan(&[1, 2, 3, 5, 8], 10), vec![1..=8]);
    }

    #[test]
    fn respects_the_maximum_length() {
        assert_eq!(plan(&[1, 2, 3, 20, 21], 10), vec![1..=3, 20..=21]);
        assert_eq!(plan(&[1, 5, 10, 11], 10), vec![1..=5, 10..=11]);
    }

    #[test]
    fn fewest_ranges_then_shortest() {
        // Two ranges are needed either way, but 1..=2 + 10..=12 is shorter than 1..=11 + 12..=12.
        assert_eq!(plan(&[1, 2, 10, 11, 12], 11), vec![1..=2, 10..=12]);
    }

    #[test]
    fn single_address_longer_than_limit_is_still_read() {
        assert_eq!(plan(&[5], 1), vec![5..=5]);
        assert_eq!(plan(&[5, 6], 1), vec![5..=5, 6..=6]);
    }
}
