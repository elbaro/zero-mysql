use crate::protocol::command::{ColumnDefinition, ColumnDefinitions};

pub struct PreparedStatement {
    id: u32,
    column_definitions: Option<ColumnDefinitions>,
}

impl PreparedStatement {
    pub fn new(id: u32) -> Self {
        PreparedStatement {
            id,
            column_definitions: None,
        }
    }
    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn column_definitions<'a>(&'a self) -> Option<&'a [ColumnDefinition<'a>]> {
        self.column_definitions.as_ref().map(|v| v.definitions())
    }

    pub fn set_column_definitions<'a>(&mut self, column_definitions: ColumnDefinitions) {
        self.column_definitions = Some(column_definitions);
    }
}
