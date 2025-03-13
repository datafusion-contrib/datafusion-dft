// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#[cfg(feature = "http")]
mod test {
    use serde::Deserialize;

    // We allow this to make sure we get the expected results
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    struct CatalogItem {
        table_catalog: String,
        table_schema: String,
        table_name: String,
        table_type: String,
    }

    #[tokio::test]
    async fn test_http_catalog() {
        let res = reqwest::get("http://localhost:8080/catalog").await.unwrap();
        let catalog = res.json::<Vec<CatalogItem>>().await.unwrap();
        assert_eq!(catalog.len(), 7);
    }

    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    struct SettingItem {
        name: String,
        value: Option<String>,
        description: String,
    }

    #[tokio::test]
    async fn test_get_table() {
        let res =
            reqwest::get("http://localhost:8080/table/datafusion/information_schema/df_settings")
                .await
                .unwrap();
        let settings = res.json::<Vec<SettingItem>>().await.unwrap();
        assert_eq!(settings.len(), 89);
    }
}
