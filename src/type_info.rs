pub trait TypeInfo: Sync + Send {
    fn type_name(&self) -> String;
}

#[macro_export]
macro_rules! impl_type_info_single_templ {
    ($name:ident$(<$($T:ident),+>)*) => {
        impl<D: IndexIdToParentData>$(<$($T: TypeInfo),*>)* TypeInfo for $name<D>$(<$($T),*>)* {
            fn type_name(&self) -> String {
                String::from(stringify!($name))
            }
        }
    }
}
