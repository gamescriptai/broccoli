use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, ReturnType, Type};

#[proc_macro_attribute]
pub fn handle_result(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);

    // Extract function details
    let fn_vis = &input_fn.vis;
    let fn_name = &input_fn.sig.ident;
    let fn_generics = &input_fn.sig.generics;
    let fn_inputs = &input_fn.sig.inputs;
    let fn_block = &input_fn.block;
    let is_async = input_fn.sig.asyncness.is_some();

    // Ensure the first argument is of type BroccoliQueue
    let first_arg_type = match fn_inputs.first() {
        Some(syn::FnArg::Typed(pat_type)) => &pat_type.ty,
        _ => panic!("First argument must be of type BroccoliQueue"),
    };

    let broccoli_queue_check = match **first_arg_type {
        Type::Path(ref type_path)
            if type_path.path.segments.last().unwrap().ident == "BroccoliQueue" =>
        {
            quote! {}
        }
        _ => panic!("First argument must be of type BroccoliQueue"),
    };

    // Extract return type if it's a Result
    let return_type = match &input_fn.sig.output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => quote! { #ty },
    };

    let wrapped_fn = if is_async {
        quote! {
            #fn_vis async fn #fn_name #fn_generics(#fn_inputs) #return_type {
                #broccoli_queue_check
                let result = async move #fn_block.await;

                match result {
                    Ok(_) => {
                        log::info!("Function {} completed successfully", stringify!(#fn_name));
                        result
                    }
                    Err(e) => {
                        log::error!("Function {} failed with error: {:?}", stringify!(#fn_name), e);
                        result
                    }
                }
            }
        }
    } else {
        quote! {
            #fn_vis fn #fn_name #fn_generics(#fn_inputs) #return_type {
                #broccoli_queue_check
                let result = (|| #fn_block)();

                match result {
                    Ok(_) => {
                        log::info!("Function {} completed successfully", stringify!(#fn_name));
                        result
                    }
                    Err(e) => {
                        log::error!("Function {} failed with error: {:?}", stringify!(#fn_name), e);
                        result
                    }
                }
            }
        }
    };

    TokenStream::from(wrapped_fn)
}
