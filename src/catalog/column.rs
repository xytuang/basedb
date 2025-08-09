use crate::common::types::types::TypeId;

pub struct Column {
    column_name: String,
    column_type: TypeId,
    length: u32,
    column_offset: u32,
}
