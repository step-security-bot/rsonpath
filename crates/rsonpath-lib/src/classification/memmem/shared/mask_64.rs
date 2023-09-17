use crate::{debug, input::Input, query::JsonString};

pub(crate) fn find_in_mask<I: Input>(
    input: &I,
    label: &JsonString,
    previous_block: u64,
    first: u64,
    second: u64,
    offset: usize,
) -> Option<usize> {
    let label_size = label.bytes_with_quotes().len();
    let mut result = (previous_block | (first << 1)) & second;
    while result != 0 {
        let idx = result.trailing_zeros() as usize;
        debug!("{offset} + {idx} - 2 to {offset} + {idx} + {label_size} - 3");
        if input.is_member_match(offset + idx - 2, offset + idx + label_size - 3, label) {
            return Some(offset + idx - 2);
        }
        result &= !(1 << idx);
    }
    None
}
