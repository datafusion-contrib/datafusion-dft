use std::{
    path::Path,
    sync::{Arc, Weak},
};

use arrow_udf_wasm::Runtime;
use datafusion::{
    arrow::{
        array::{ArrayRef, Float64Array},
        datatypes::DataType,
    },
    common::{cast::as_float64_array, exec_err},
    error::{DataFusionError, Result},
    execution::context::{FunctionFactory, RegisterFunction, SessionState},
    logical_expr::{
        create_udf, ColumnarValue, CreateFunction, DefinitionStatement, ScalarUDF, Volatility,
    },
};
use thiserror::Error;
use tokio::sync::Mutex;
// use wasmedge_sdk::{config::ConfigBuilder, dock::VmDock, Module, VmBuilder};
use weak_table::WeakValueHashMap;

mod udf;

// type ModuleCache = Arc<Mutex<WeakValueHashMap<String, Weak<VmDock>>>>;

fn test_udf() -> ScalarUDF {
    // First, declare the actual implementation of the calculation
    let pow = Arc::new(|args: &[ColumnarValue]| {
        // in DataFusion, all `args` and output are dynamically-typed arrays, which means that we need to:
        // 1. cast the values to the type we want
        // 2. perform the computation for every element in the array (using a loop or SIMD) and construct the result

        // this is guaranteed by DataFusion based on the function's signature.
        assert_eq!(args.len(), 2);

        // Expand the arguments to arrays (this is simple, but inefficient for
        // single constant values).
        let args = ColumnarValue::values_to_arrays(args)?;

        // 1. cast both arguments to f64. These casts MUST be aligned with the signature or this function panics!
        let base = as_float64_array(&args[0]).expect("cast failed");
        let exponent = as_float64_array(&args[1]).expect("cast failed");

        // The array lengths is guaranteed by DataFusion. We assert here to make it obvious.
        assert_eq!(exponent.len(), base.len());

        // 2. perform the computation
        let array = base
            .iter()
            .zip(exponent.iter())
            .map(|(base, exponent)| {
                match (base, exponent) {
                    // in arrow, any value can be null.
                    // Here we decide to make our UDF to return null when either base or exponent is null.
                    (Some(base), Some(exponent)) => Some(base.powf(exponent)),
                    _ => None,
                }
            })
            .collect::<Float64Array>();

        // `Ok` because no error occurred during the calculation (we should add one if exponent was [0, 1[ and the base < 0 because that panics!)
        // `Arc` because arrays are immutable, thread-safe, trait objects.
        Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
    });

    // Next:
    // * give it a name so that it shows nicely when the plan is printed
    // * declare what input it expects
    // * declare its return type
    let pow = create_udf(
        "f1",
        // expects two f64
        vec![DataType::Float64, DataType::Float64],
        // returns f64
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        pow,
    );
    pow
}

pub struct WasmFunctionFactory {
    // note:
    // https://github.com/WasmEdge/wasmedge-rust-sdk/issues/89
    // comments do not add up to VM interface, on top of it
    // UDFs do not modify any state. leaving as it is for now
    // may revert it later
    // modules: ModuleCache,
}

#[async_trait::async_trait]
impl FunctionFactory for WasmFunctionFactory {
    async fn create(
        &self,
        _state: &SessionState,
        statement: CreateFunction,
    ) -> Result<RegisterFunction> {
        let return_type = statement.return_type.expect("return type expected");
        let argument_types = statement
            .args
            .map(|args| {
                args.into_iter()
                    .map(|a| a.data_type)
                    .collect::<Vec<DataType>>()
            })
            .unwrap_or_default();
        let declared_name = statement.name;
        let (module_name, method_name) = match &statement.params.as_ {
            Some(DefinitionStatement::SingleQuotedDef(path)) => {
                println!("Got create function path: {}", path);
                Self::wasm_module_function(path)?
            }
            None => return exec_err!("wasm function not defined "),
            Some(f) => return exec_err!("wasm function incorrect {:?} ", f),
        };

        // let rt = Runtime::new(binary);

        // let vm = self.wasm_model_cache_or_load(&module_name).await?;
        // let f = crate::udf::WasmFunctionWrapper::new(
        //     vm,
        //     declared_name,
        //     method_name,
        //     argument_types,
        //     return_type,
        // )?;

        let f = test_udf();

        Ok(RegisterFunction::Scalar(Arc::new(ScalarUDF::from(f))))
    }
}

impl Default for WasmFunctionFactory {
    fn default() -> Self {
        WasmFunctionFactory {
            // modules: Arc::new(Mutex::new(WeakValueHashMap::new())),
        }
    }
}

impl WasmFunctionFactory {
    /// returns cached module or
    /// loads, caches module and returns module
    /// for given module path
    // async fn wasm_model_cache_or_load(
    //     &self,
    //     wasm_module_path: &str,
    // ) -> std::result::Result<Arc<VmDock>, WasmFunctionError> {
    //     // caching key is bit primitive, but good enough for now
    //     let mut modules = self.modules.lock().await;
    //     // lets assume creation of new module will not take too long
    //     // and lock will be kept for a very short period of time,
    //     // good enough for now
    //     match modules.get(wasm_module_path) {
    //         Some(module) => {
    //             log::debug!("return cached VM for wasm_module={}", wasm_module_path);
    //             Ok(module.clone())
    //         }
    //         None => {
    //             log::debug!("no cached VM for wasm_module={}", wasm_module_path);
    //             let module = Self::wasm_model_load(wasm_module_path)?;
    //             modules.insert(wasm_module_path.to_string(), module.clone());
    //             Ok(module)
    //         }
    //     }
    // }

    fn wasm_module_function(s: &str) -> Result<(String, String)> {
        match s.split('!').collect::<Vec<&str>>()[..] {
            [module, method] if !module.is_empty() && !method.is_empty() => {
                Ok((module.to_string(), method.to_string()))
            }
            _ => exec_err!("bad module/method format"),
        }
    }

    // fn wasm_model_load(wasm_module: &str) -> std::result::Result<Arc<VmDock>, WasmFunctionError> {
    //     log::debug!("producing new VM for wasm_module={}", wasm_module);
    //     let file = Path::new(&wasm_module);
    //     let module = if file.is_absolute() {
    //         Module::from_file(None, wasm_module)?
    //     } else {
    //         let mut project_root = project_root::get_project_root()
    //             .map_err(|e| WasmFunctionError::Execution(e.to_string()))?;
    //         project_root.push(file);
    //         Module::from_file(None, &project_root)?
    //     };
    //
    //     // default configuration will do for now
    //     let config = ConfigBuilder::default().build()?;
    //
    //     let vm = VmBuilder::new()
    //         .with_config(config)
    //         .build()?
    //         .register_module(None, module)?;
    //
    //     Ok(Arc::new(VmDock::new(vm)))
    // }
    // #[cfg(test)]
    // fn module_cache(&self) -> ModuleCache {
    //     self.modules.clone()
    // }
}

// #[derive(Error, Debug)]
// pub enum WasmFunctionError {
//     #[error("WasmEdge Error: {0}")]
//     WasmEdgeError(#[from] Box<wasmedge_sdk::error::WasmEdgeError>),
//     #[error("Execution Error: {0}")]
//     Execution(String),
// }

// impl From<WasmFunctionError> for DataFusionError {
//     fn from(e: WasmFunctionError) -> Self {
//         // will do for now
//         DataFusionError::Execution(e.to_string())
//     }
// }

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::{
        arrow::array::{ArrayRef, Float64Array, RecordBatch},
        assert_batches_eq,
        execution::context::SessionContext,
    };

    use crate::WasmFunctionFactory;

    #[test]
    fn test_module_function_split() {
        let (module, method) = WasmFunctionFactory::wasm_module_function("module!method").unwrap();
        assert_eq!("module", module);
        assert_eq!("method", method);

        assert!(WasmFunctionFactory::wasm_module_function("!method").is_err());
    }
    #[tokio::test]
    async fn should_handle_happy_path() -> datafusion::error::Result<()> {
        let ctx =
            SessionContext::new().with_function_factory(Arc::new(WasmFunctionFactory::default()));

        let a: ArrayRef = Arc::new(Float64Array::from(vec![2.0, 3.0, 4.0, 5.0]));
        let b: ArrayRef = Arc::new(Float64Array::from(vec![2.0, 3.0, 4.0, 5.1]));
        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)])?;

        ctx.register_batch("t", batch)?;

        let sql = r#"
        CREATE FUNCTION f1(DOUBLE, DOUBLE)
        RETURNS DOUBLE
        LANGUAGE WASM
        AS 'wasm_function/target/wasm32-unknown-unknown/debug/wasm_function.wasm!f1'
        "#;

        ctx.sql(sql).await?.show().await?;

        let result = ctx
            .sql("select a, b, f1(a,b) from t")
            .await?
            .collect()
            .await?;
        let expected = vec![
            "+-----+-----+-------------------+",
            "| a   | b   | f1(t.a,t.b)       |",
            "+-----+-----+-------------------+",
            "| 2.0 | 2.0 | 4.0               |",
            "| 3.0 | 3.0 | 27.0              |",
            "| 4.0 | 4.0 | 256.0             |",
            "| 5.0 | 5.1 | 3670.684197150057 |",
            "+-----+-----+-------------------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    async fn should_handle_error() -> datafusion::error::Result<()> {
        let ctx =
            SessionContext::new().with_function_factory(Arc::new(WasmFunctionFactory::default()));

        let sql = r#"
        CREATE FUNCTION f2(DOUBLE, DOUBLE)
        RETURNS DOUBLE
        LANGUAGE WASM
        AS 'wasm_function/target/wasm32-unknown-unknown/debug/wasm_function.wasm!f_return_error'
        "#;

        ctx.sql(sql).await?.show().await?;

        let result = ctx.sql("select f2(1.0,1.0)").await?.show().await;

        assert!(result.is_err());
        assert_eq!(
            "Execution error: [Wasm Invocation] wasm function returned error",
            result.err().unwrap().to_string()
        );

        Ok(())
    }

    #[tokio::test]
    async fn should_handle_arrow_error() -> datafusion::error::Result<()> {
        let ctx =
            SessionContext::new().with_function_factory(Arc::new(WasmFunctionFactory::default()));

        let sql = r#"
        CREATE FUNCTION f2(DOUBLE, DOUBLE)
        RETURNS DOUBLE
        LANGUAGE WASM
        AS 'wasm_function/target/wasm32-unknown-unknown/debug/wasm_function.wasm!f_return_arrow_error'
        "#;

        ctx.sql(sql).await?.show().await?;

        let result = ctx.sql("select f2(1.0,1.0)").await?.show().await;

        assert!(result.is_err());
        assert_eq!(
            "Execution error: [Wasm Invocation] Divide by zero error",
            result.err().unwrap().to_string()
        );

        Ok(())
    }

    #[tokio::test]
    #[ignore = "WasmEdge does not handle panic after latest change"]
    async fn should_handle_panic() -> datafusion::error::Result<()> {
        let ctx =
            SessionContext::new().with_function_factory(Arc::new(WasmFunctionFactory::default()));

        let sql = r#"
        CREATE FUNCTION f1(DOUBLE, DOUBLE)
        RETURNS DOUBLE
        LANGUAGE WASM
        AS 'wasm_function/target/wasm32-unknown-unknown/debug/wasm_function.wasm!f1'
        "#;
        // we register good function to verify that panich
        // will not put vm to some unexpected state
        ctx.sql(sql).await?.show().await?;

        let sql = r#"
        CREATE FUNCTION f3(DOUBLE, DOUBLE)
        RETURNS DOUBLE
        LANGUAGE WASM
        AS 'wasm_function/target/wasm32-unknown-unknown/debug/wasm_function.wasm!f_panic'
        "#;

        ctx.sql(sql).await?.show().await?;

        let result = ctx.sql("select f3(1.0,1.0)").await?.show().await;

        assert!(result.is_err());
        assert_eq!(
            "Execution error: [Wasm Invocation Panic] unreachable",
            result.err().unwrap().to_string()
        );
        let result = ctx.sql("select f1(1.0,1.0)").await?.collect().await?;
        let expected = vec![
            "+---------------------------+",
            "| f1(Float64(1),Float64(1)) |",
            "+---------------------------+",
            "| 1.0                       |",
            "+---------------------------+",
        ];

        assert_batches_eq!(expected, &result);
        Ok(())
    }

    #[tokio::test]
    async fn should_create_drop_function() -> datafusion::error::Result<()> {
        let function_factory = Arc::new(WasmFunctionFactory::default());
        let ctx = SessionContext::new().with_function_factory(function_factory.clone());

        let sql = r#"
        CREATE FUNCTION f1(DOUBLE, DOUBLE)
        RETURNS DOUBLE
        LANGUAGE WASM
        AS 'wasm_function/target/wasm32-unknown-unknown/debug/wasm_function.wasm!f1'
        "#;

        ctx.sql(sql).await?.show().await?;

        let sql = r#"
        CREATE FUNCTION f2(DOUBLE, DOUBLE)
        RETURNS DOUBLE
        LANGUAGE WASM
        AS 'wasm_function/target/wasm32-unknown-unknown/debug/wasm_function.wasm!f_return_arrow_error'
        "#;

        ctx.sql(sql).await?.show().await?;

        let result = ctx.sql("select f1(2.0,2.0)").await?.collect().await?;
        let expected = vec![
            "+---------------------------+",
            "| f1(Float64(2),Float64(2)) |",
            "+---------------------------+",
            "| 4.0                       |",
            "+---------------------------+",
        ];

        assert_batches_eq!(expected, &result);

        // we should have one modules caching
        // assert_eq!(1, function_factory.module_cache().lock().await.len());

        let sql = r#"
        DROP FUNCTION f1
        "#;

        ctx.sql(sql).await?.show().await?;

        let sql = r#"
        DROP FUNCTION f2
        "#;

        ctx.sql(sql).await?.show().await?;

        // we should have none modules cached
        // weak hashmap should drop VM after last function
        // has been dropped.
        // note, weak hash map is lazy to drop
        // assert_eq!(
        //     0,
        //     function_factory
        //         .module_cache()
        //         .lock()
        //         .await
        //         .keys()
        //         .collect::<Vec<_>>()
        //         .len()
        // );

        Ok(())
    }
}

// #[cfg(test)]
// #[ctor::ctor]
// fn init() {
//     // Enable RUST_LOG logging configuration for test
//     let _ = env_logger::builder().is_test(true).try_init();
// }
