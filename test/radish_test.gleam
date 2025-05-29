import radish
import radish/error

import gleeunit

pub fn main() {
  gleeunit.main()
}

fn get_test_client(next) {
  let assert Ok(client) = radish.start("localhost", 6379, [radish.Timeout(128)])

  let res = next(client)
  radish.shutdown(client)
  res
}

pub fn roundtrip_test() {
  use client <- get_test_client()

  let assert Error(error.NotFound) = client |> radish.get("key", 1000)

  let assert Ok("OK") =
    client
    |> radish.set("key", "value", 1000)

  let assert Ok("value") = client |> radish.get("key", 1000)
  let assert Error(error.NotFound) = client |> radish.get("key2", 1000)
  let assert Ok(1) = client |> radish.del(["key"], 1000)
  let assert Error(error.NotFound) = client |> radish.get("key", 1000)
}
