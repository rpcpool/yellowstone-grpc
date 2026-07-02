use {
    anyhow::Result,
    yellowstone_grpc_e2e_macros::test_helper,
    crate::scenarios::RunConfig,
};

/// Example scenario in a second file — shows up automatically in `yellowstone-e2e list`.
#[test_helper(name = "example", tags = ["example"])]
pub async fn example_scenario(_config: &RunConfig) -> Result<()> {
    Ok(())
}
