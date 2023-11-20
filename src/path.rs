use lemmy_api_common::sensitive::Sensitive;
use serde::{de::DeserializeOwned, Serialize};

pub trait Params: Serialize {
    fn with_auth(self, jwt: Option<Sensitive<String>>) -> Self;
}

pub trait AuthParams: Params {
    fn new() -> Self;
}

pub trait Path {
    type Params: Params;
    type Response: DeserializeOwned;

    fn path(&self) -> &'static str;
}

#[macro_export]
macro_rules! params {
    ($name:ident) => {
        impl $crate::path::Params for $name {
            fn with_auth(self, jwt: Option<Sensitive<String>>) -> Self {
                $name { auth: jwt, ..self }
            }
        }
    };
}

#[macro_export]
macro_rules! auth_params {
    ($name:ident) => {
        impl $crate::path::Params for $name {
            fn with_auth(self, jwt: Option<Sensitive<String>>) -> Self {
                $name { auth: jwt }
            }
        }

        impl $crate::path::AuthParams for $name {
            fn new() -> Self {
                $name { auth: None }
            }
        }
    };
}

#[macro_export]
macro_rules! path_impl {
    ($name:ident, $path:expr, $params:ident, $response:ty) => {
        pub struct $name;

        impl $crate::path::Path for $name {
            type Params = $params;
            type Response = $response;

            fn path(&self) -> &'static str {
                $path
            }
        }
    };
}

#[macro_export]
macro_rules! path {
    ($name:ident, $path:expr, $params:ident(Auth), $response:ty) => {
        $crate::auth_params!($params);

        $crate::path_impl!($name, $path, $params, $response);
    };
    ($name:ident, $path:expr, $params:ident, $response:ty) => {
        $crate::params!($params);

        $crate::path_impl!($name, $path, $params, $response);
    };
}
