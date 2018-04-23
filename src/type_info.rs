pub trait TypeInfo: Sync + Send {
    fn type_name(&self) -> String;
}

#[macro_export]
macro_rules! impl_type_info {
    ($($name:ident$(<$($T:ident),+>)*),*) => {
        $(impl_type_info_single!($name$(<$($T),*>)*);)*
    };
}

#[macro_export]
macro_rules! mut_if {
    ($name: ident = $value: expr, $($any: expr) +) => {
        let mut $name = $value;
    };
    ($name: ident = $value: expr,) => {
        let $name = $value;
    };
}

#[macro_export]
macro_rules! impl_type_info_single {
    ($name:ident$(<$($T:ident),+>)*) => {
        impl$(<$($T: TypeInfo),*>)* TypeInfo for $name$(<$($T),*>)* {
            fn type_name(&self) -> String {
                mut_if!(res = String::from(stringify!($name)), $($($T)*)*);
                $(
                    res.push('<');
                    $(
                        res.push_str(&$T::type_name());
                        res.push(',');
                    )*
                    res.pop();
                    res.push('>');
                )*
                res
            }
        }
    }
}

#[macro_export]
macro_rules! impl_type_info_single_templ {
    ($name:ident$(<$($T:ident),+>)*) => {
        impl<D: IndexIdToParentData>$(<$($T: TypeInfo),*>)* TypeInfo for $name<D>$(<$($T),*>)* {
            fn type_name(&self) -> String {
                mut_if!(res = String::from(stringify!($name)), $($($T)*)*);
                $(
                    res.push('<');
                    $(
                        res.push_str(&$T::type_name(&self));
                        res.push(',');
                    )*
                    res.pop();
                    res.push('>');
                )*
                res
            }
        }
    }
}
