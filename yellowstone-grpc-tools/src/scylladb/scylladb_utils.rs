use scylla::{
    cql_to_rust::{FromCqlVal, FromCqlValError, FromRowError},
    FromRow,
};

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct LwtResult(pub bool);

impl FromRow for LwtResult {
    fn from_row(
        row: scylla::frame::response::result::Row,
    ) -> Result<Self, scylla::cql_to_rust::FromRowError> {
        row.columns
            .first()
            .ok_or(FromRowError::BadCqlVal {
                err: FromCqlValError::ValIsNull,
                column: 0,
            })
            .and_then(|cqlval| {
                bool::from_cql(cqlval.to_owned()).map_err(|_err| FromRowError::BadCqlVal {
                    err: FromCqlValError::BadCqlType,
                    column: 0,
                })
            })
            .map(LwtResult)
    }
}
