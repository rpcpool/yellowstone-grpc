use {
    serde::{de, Deserialize, Deserializer},
    std::{fmt, str::FromStr},
};

#[derive(Deserialize)]
#[serde(untagged)]
pub enum ValueIntStr<'a, T> {
    Int(T),
    Str(&'a str),
}

pub fn deserialize_int_str<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + FromStr,
    <T as FromStr>::Err: fmt::Display,
{
    match ValueIntStr::<T>::deserialize(deserializer)? {
        ValueIntStr::Int(value) => Ok(value),
        ValueIntStr::Str(value) => value
            .replace('_', "")
            .parse::<T>()
            .map_err(de::Error::custom),
    }
}
