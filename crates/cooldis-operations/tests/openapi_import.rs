use cooldis_operations::{
    ImportPackageSource, OpenApiImportError, OperationImportPlan, OperationParameterLocation,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[test]
fn operation_id_and_fallback_map_to_stable_operation_names() {
    let root = temp_dir("names");
    let package = write_package(
        &root,
        json!({
            "openapi": "3.1.0",
            "info": {"title": "Names", "version": "1"},
            "servers": [{"url": "https://api.example.com"}],
            "paths": {
                "/search": {
                    "post": {
                        "operationId": "searchThings",
                        "responses": {"200": {"description": "ok"}}
                    }
                },
                "/users/{user-id}": {
                    "get": {
                        "parameters": [{
                            "name": "user-id",
                            "in": "path",
                            "required": true,
                            "schema": {"type": "string"}
                        }],
                        "responses": {"200": {"description": "ok"}}
                    }
                }
            }
        }),
        "",
        &[
            ("searchThings", None, None),
            ("get_users_user_id", None, None),
        ],
    );

    let source = ImportPackageSource::load(package).unwrap();
    let plan = OperationImportPlan::from_package(&source).unwrap();

    assert_eq!(plan.operations[0].name, "searchThings");
    assert_eq!(plan.operations[1].name, "get_users_user_id");
    assert_eq!(plan.operations[1].path_template, "/users/{user-id}");
    assert_eq!(
        plan.operations[1].parameters[0].location,
        OperationParameterLocation::Path
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn duplicate_projected_names_require_aliases() {
    let root = temp_dir("duplicates");
    let spec = json!({
        "openapi": "3.0.3",
        "info": {"title": "Duplicates", "version": "1"},
        "servers": [{"url": "https://api.example.com"}],
        "paths": {
            "/first": {
                "get": {
                    "operationId": "find",
                    "responses": {"200": {"description": "ok"}}
                }
            },
            "/second": {
                "get": {
                    "operationId": "find",
                    "responses": {"200": {"description": "ok"}}
                }
            }
        }
    });
    let package = write_package(
        &root,
        spec.clone(),
        "",
        &[("find", None, None), ("find", None, None)],
    );
    let source = ImportPackageSource::load(package).unwrap();
    assert!(matches!(
        OperationImportPlan::from_package(&source),
        Err(OpenApiImportError::DuplicateProjectedName { .. })
    ));

    let package = write_package(
        &root,
        spec,
        "",
        &[
            ("find", Some("find_first"), None),
            ("find", Some("find_second"), None),
        ],
    );
    let source = ImportPackageSource::load(package).unwrap();
    let plan = OperationImportPlan::from_package(&source).unwrap();
    assert_eq!(
        plan.operations
            .iter()
            .map(|operation| operation.name.as_str())
            .collect::<Vec<_>>(),
        ["find_first", "find_second"]
    );

    let package = write_package(
        &root,
        json!({
            "openapi": "3.0.3",
            "info": {"title": "Projection collision", "version": "1"},
            "servers": [{"url": "https://api.example.com"}],
            "paths": {
                "/first": {
                    "get": {
                        "operationId": "first",
                        "responses": {"200": {"description": "ok"}}
                    }
                },
                "/second": {
                    "get": {
                        "operationId": "second",
                        "responses": {"200": {"description": "ok"}}
                    }
                }
            }
        }),
        "",
        &[
            ("first", Some("find-result"), None),
            ("second", Some("find_result"), None),
        ],
    );
    let source = ImportPackageSource::load(package).unwrap();
    assert!(matches!(
        OperationImportPlan::from_package(&source),
        Err(OpenApiImportError::DuplicateProjectedName { .. })
    ));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn path_query_header_body_and_api_key_auth_lower_into_the_plan() {
    let root = temp_dir("mapping");
    let package = write_package(
        &root,
        mapped_spec(),
        "[auth]\nscheme = \"apiKey\"\nheader = \"x-api-key\"\nsecret = \"SEARCH_API_KEY\"\n",
        &[("search", None, Some("Search the catalog."))],
    );

    let source = ImportPackageSource::load(package).unwrap();
    let plan = OperationImportPlan::from_package(&source).unwrap();
    let operation = &plan.operations[0];

    assert_eq!(
        operation.description.as_deref(),
        Some("Search the catalog.")
    );
    assert_eq!(
        operation
            .parameters
            .iter()
            .map(|parameter| (&parameter.name, &parameter.location))
            .collect::<Vec<_>>(),
        [
            (&"collection".to_string(), &OperationParameterLocation::Path),
            (&"limit".to_string(), &OperationParameterLocation::Query),
            (&"x-client".to_string(), &OperationParameterLocation::Header),
        ]
    );
    assert_eq!(
        operation
            .request_body
            .as_ref()
            .unwrap()
            .input_property
            .as_deref(),
        Some("body")
    );
    assert!(
        operation
            .required_capabilities
            .contains("secret:SEARCH_API_KEY")
    );
    assert_eq!(operation.secret_headers[0].name, "x-api-key");
    assert_eq!(operation.secret_headers[0].secret, "SEARCH_API_KEY");
    assert_eq!(operation.secret_headers[0].prefix, "");
    assert_eq!(
        operation.input_schema["required"],
        json!(["collection", "body"])
    );
    assert_eq!(
        operation.input_schema["properties"]["body"]["required"],
        json!(["query"])
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn bearer_auth_uses_the_authorization_header_and_bearer_prefix() {
    let root = temp_dir("bearer");
    let package = write_package(
        &root,
        mapped_spec(),
        "[auth]\nscheme = \"bearer\"\nheader = \"Authorization\"\nsecret = \"SEARCH_TOKEN\"\n",
        &[("search", None, None)],
    );

    let source = ImportPackageSource::load(package).unwrap();
    let plan = OperationImportPlan::from_package(&source).unwrap();
    let auth = &plan.operations[0].secret_headers[0];
    assert_eq!(auth.name, "Authorization");
    assert_eq!(auth.secret, "SEARCH_TOKEN");
    assert_eq!(auth.prefix, "Bearer ");
    assert!(
        plan.operations[0]
            .required_capabilities
            .contains("secret:SEARCH_TOKEN")
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn import_rejects_credential_header_collisions_and_invalid_secret_names() {
    let root = temp_dir("auth-header-collision");
    let mut spec = mapped_spec();
    spec["paths"]["/{collection}/search"]["post"]["parameters"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "name": "X-API-Key",
            "in": "header",
            "required": false,
            "schema": {"type": "string"}
        }));
    let package = write_package(
        &root,
        spec,
        "[auth]\nscheme = \"apiKey\"\nheader = \"x-api-key\"\nsecret = \"SEARCH_API_KEY\"\n",
        &[("search", None, None)],
    );
    let source = ImportPackageSource::load(package).unwrap();
    assert!(matches!(
        OperationImportPlan::from_package(&source),
        Err(OpenApiImportError::CredentialHeaderCollision { .. })
    ));
    let _ = fs::remove_dir_all(root);

    let root = temp_dir("invalid-secret-name");
    let package = write_package(
        &root,
        mapped_spec(),
        "[auth]\nscheme = \"apiKey\"\nheader = \"x-api-key\"\nsecret = \" SEARCH_API_KEY \"\n",
        &[("search", None, None)],
    );
    assert!(matches!(
        ImportPackageSource::load(package),
        Err(OpenApiImportError::InvalidAuthentication { .. })
    ));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn import_rejects_reserved_or_malformed_header_parameters() {
    for header in ["Authorization", "Host", "bad header"] {
        let root = temp_dir("reserved-header");
        let mut spec = mapped_spec();
        spec["paths"]["/{collection}/search"]["post"]["parameters"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "name": header,
                "in": "header",
                "required": false,
                "schema": {"type": "string"}
            }));
        let package = write_package(&root, spec, "", &[("search", None, None)]);
        let source = ImportPackageSource::load(package).unwrap();
        assert!(matches!(
            OperationImportPlan::from_package(&source),
            Err(OpenApiImportError::UnsupportedHeaderParameter { .. })
        ));
        let _ = fs::remove_dir_all(root);
    }
}

#[test]
fn optional_body_only_operations_expose_an_omittable_body_field() {
    let root = temp_dir("optional-body");
    let mut spec = mapped_spec();
    let path_item = spec["paths"]
        .as_object_mut()
        .unwrap()
        .remove("/{collection}/search")
        .unwrap();
    spec["paths"]["/search"] = path_item;
    let operation = &mut spec["paths"]["/search"]["post"];
    operation["parameters"] = json!([]);
    operation["requestBody"]["required"] = json!(false);
    let package = write_package(&root, spec, "", &[("search", None, None)]);
    let source = ImportPackageSource::load(package).unwrap();
    let plan = OperationImportPlan::from_package(&source).unwrap();
    let operation = &plan.operations[0];

    assert_eq!(
        operation
            .request_body
            .as_ref()
            .unwrap()
            .input_property
            .as_deref(),
        Some("body")
    );
    assert_eq!(operation.input_schema["type"], "object");
    assert_eq!(operation.input_schema["required"], json!([]));
    assert!(operation.input_schema["properties"].get("body").is_some());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn server_urls_are_canonicalized_and_special_ip_ranges_request_private_grants() {
    let root = temp_dir("canonical-server");
    let package = write_package(
        &root,
        mapped_spec_with_server("HTTP://127.0.0.1:80/v1/"),
        "",
        &[("search", None, None)],
    );
    let source = ImportPackageSource::load(package).unwrap();
    let plan = OperationImportPlan::from_package(&source).unwrap();
    let operation = &plan.operations[0];
    assert_eq!(operation.server_url, "http://127.0.0.1/v1");
    assert_eq!(operation.origin, "http://127.0.0.1");
    assert!(
        operation
            .required_capabilities
            .contains("net.http.private:POST:http://127.0.0.1")
    );
    let _ = fs::remove_dir_all(root);

    let root = temp_dir("link-local-server");
    let package = write_package(
        &root,
        mapped_spec_with_server("http://169.254.10.20"),
        "",
        &[("search", None, None)],
    );
    let source = ImportPackageSource::load(package).unwrap();
    let plan = OperationImportPlan::from_package(&source).unwrap();
    assert!(
        plan.operations[0]
            .required_capabilities
            .contains("net.http.private:POST:http://169.254.10.20")
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn malformed_versions_and_path_templates_fail_closed() {
    let root = temp_dir("version");
    let mut spec = mapped_spec();
    spec["openapi"] = json!("3.0.not-a-version");
    let package = write_package(&root, spec, "", &[("search", None, None)]);
    let source = ImportPackageSource::load(package).unwrap();
    assert!(matches!(
        OperationImportPlan::from_package(&source),
        Err(OpenApiImportError::UnsupportedVersion { .. })
    ));
    let _ = fs::remove_dir_all(root);

    let root = temp_dir("path-template");
    let mut spec = mapped_spec();
    let paths = spec["paths"].as_object_mut().unwrap();
    let operation = paths.remove("/{collection}/search").unwrap();
    paths.insert("not/absolute".to_string(), operation);
    let package = write_package(&root, spec, "", &[("search", None, None)]);
    let source = ImportPackageSource::load(package).unwrap();
    assert!(matches!(
        OperationImportPlan::from_package(&source),
        Err(OpenApiImportError::InvalidPathTemplate { .. })
    ));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn package_and_operation_names_must_already_be_canonical() {
    let root = temp_dir("noncanonical-alias");
    let package = write_package(
        &root,
        mapped_spec(),
        "",
        &[("search", Some(" search_alias "), None)],
    );
    assert!(matches!(
        ImportPackageSource::load(package),
        Err(OpenApiImportError::InvalidOperationName { .. })
    ));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn unsupported_auth_callbacks_and_multipart_are_typed_errors() {
    for scheme in ["basic", "oauth2", "openIdConnect"] {
        let root = temp_dir(scheme);
        let auth = format!(
            "[auth]\nscheme = {scheme:?}\nheader = \"Authorization\"\nsecret = \"TOKEN\"\n"
        );
        let package = write_package(&root, mapped_spec(), &auth, &[("search", None, None)]);
        let source = ImportPackageSource::load(package).unwrap();
        assert!(matches!(
            OperationImportPlan::from_package(&source),
            Err(OpenApiImportError::UnsupportedAuthentication { .. })
        ));
        let _ = fs::remove_dir_all(root);
    }

    let root = temp_dir("callbacks");
    let mut spec = mapped_spec();
    spec["paths"]["/{collection}/search"]["post"]["callbacks"] = json!({"done": {}});
    let package = write_package(&root, spec, "", &[("search", None, None)]);
    let source = ImportPackageSource::load(package).unwrap();
    assert!(matches!(
        OperationImportPlan::from_package(&source),
        Err(OpenApiImportError::CallbacksUnsupported { .. })
    ));
    let _ = fs::remove_dir_all(root);

    let root = temp_dir("multipart");
    let mut spec = mapped_spec();
    let body = spec["paths"]["/{collection}/search"]["post"]
        .as_object_mut()
        .unwrap()
        .get_mut("requestBody")
        .unwrap();
    body["content"] = json!({
        "multipart/form-data": {
            "schema": {"type": "object", "additionalProperties": true}
        }
    });
    let package = write_package(&root, spec, "", &[("search", None, None)]);
    let source = ImportPackageSource::load(package).unwrap();
    assert!(matches!(
        OperationImportPlan::from_package(&source),
        Err(OpenApiImportError::MultipartUnsupported { .. })
    ));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn unsupported_schema_keywords_and_spec_hash_mismatches_fail_closed() {
    let root = temp_dir("schema");
    let mut spec = mapped_spec();
    spec["paths"]["/{collection}/search"]["post"]["requestBody"]["content"]["application/json"]["schema"]
        ["oneOf"] = json!([{"type": "object"}, {"type": "string"}]);
    let package = write_package(&root, spec, "", &[("search", None, None)]);
    let source = ImportPackageSource::load(package).unwrap();
    assert!(matches!(
        OperationImportPlan::from_package(&source),
        Err(OpenApiImportError::UnsupportedSchema { .. })
    ));
    let _ = fs::remove_dir_all(root);

    let root = temp_dir("hash");
    fs::create_dir_all(&root).unwrap();
    let spec_path = root.join("openapi.json");
    fs::write(
        &spec_path,
        serde_json::to_vec_pretty(&mapped_spec()).unwrap(),
    )
    .unwrap();
    let package_path = root.join("catalog.import.toml");
    fs::write(
        &package_path,
        package_toml(&"0".repeat(64), "", &[("search", None, None)]),
    )
    .unwrap();
    assert!(matches!(
        ImportPackageSource::load(package_path),
        Err(OpenApiImportError::SpecHashMismatch { .. })
    ));
    let _ = fs::remove_dir_all(root);
}

fn mapped_spec() -> Value {
    json!({
        "openapi": "3.0.3",
        "info": {"title": "Search", "version": "1", "description": "Search API"},
        "servers": [{"url": "https://api.example.com/v1"}],
        "paths": {
            "/{collection}/search": {
                "post": {
                    "operationId": "search",
                    "description": "Search.",
                    "parameters": [
                        {
                            "name": "collection",
                            "in": "path",
                            "required": true,
                            "schema": {"type": "string"}
                        },
                        {
                            "name": "limit",
                            "in": "query",
                            "required": false,
                            "schema": {"type": "integer"}
                        },
                        {
                            "name": "x-client",
                            "in": "header",
                            "required": false,
                            "schema": {"type": "string"}
                        }
                    ],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["query"],
                                    "properties": {"query": {"type": "string"}},
                                    "additionalProperties": false
                                }
                            }
                        }
                    },
                    "responses": {"200": {"description": "ok"}}
                }
            }
        }
    })
}

fn mapped_spec_with_server(server_url: &str) -> Value {
    let mut spec = mapped_spec();
    spec["servers"][0]["url"] = json!(server_url);
    spec
}

fn write_package(
    root: &Path,
    spec: Value,
    auth: &str,
    operations: &[(&str, Option<&str>, Option<&str>)],
) -> PathBuf {
    fs::create_dir_all(root).unwrap();
    let spec_bytes = serde_json::to_vec_pretty(&spec).unwrap();
    fs::write(root.join("openapi.json"), &spec_bytes).unwrap();
    let spec_sha256 = format!("{:x}", Sha256::digest(&spec_bytes));
    let package_path = root.join("catalog.import.toml");
    fs::write(&package_path, package_toml(&spec_sha256, auth, operations)).unwrap();
    package_path
}

fn package_toml(
    spec_sha256: &str,
    auth: &str,
    operations: &[(&str, Option<&str>, Option<&str>)],
) -> String {
    let mut source = format!(
        "[import]\nname = \"catalog\"\nversion = \"1.0.0\"\ndescription = \"Catalog API\"\n\n[spec]\npath = \"openapi.json\"\nsha256 = {spec_sha256:?}\n\n{auth}"
    );
    for (operation_id, alias, description) in operations {
        source.push_str("\n[[operations]]\n");
        source.push_str(&format!("operation_id = {operation_id:?}\n"));
        if let Some(alias) = alias {
            source.push_str(&format!("alias = {alias:?}\n"));
        }
        if let Some(description) = description {
            source.push_str(&format!("description = {description:?}\n"));
        }
    }
    source
}

fn temp_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!("cooldis-openapi-{label}-{}", Uuid::now_v7()))
}
