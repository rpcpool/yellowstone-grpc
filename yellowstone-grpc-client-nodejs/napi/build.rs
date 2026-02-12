mod typegen;

fn main() {

  typegen::generate_types();

  napi_build::setup();
}
