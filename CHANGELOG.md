# Changelog

<!-- version list -->

## v3.4.0 (2025-12-03)

### Features

- Export missing public types for py.typed compliance
  ([`5f2b017`](https://github.com/Promptly-Technologies-LLC/etielle/commit/5f2b017b9279d736afb2ca113283b42ba7780300))


## v3.3.0 (2025-12-03)

### Chores

- Fix linting and type checking errors
  ([`29b2d5c`](https://github.com/Promptly-Technologies-LLC/etielle/commit/29b2d5c038984b9bc2d3664f6b9709062c8a3c95))

### Documentation

- Add monitoring/telemetry section to error handling docs
  ([`19236ec`](https://github.com/Promptly-Technologies-LLC/etielle/commit/19236ecc26aa971d61e1d9a1a107a2aeed29db2c))

### Features

- Add pipeline telemetry and stats tracking
  ([`277def1`](https://github.com/Promptly-Technologies-LLC/etielle/commit/277def191c2a7819df884a629e5a23adc887045b))


## v3.2.0 (2025-12-02)

### Documentation

- Document fk parameter for DB-generated IDs
  ([`399bc10`](https://github.com/Promptly-Technologies-LLC/etielle/commit/399bc106c88a51f5dd26e206bde99d3a8b8bd7dd))

- Fix pipe escaping in table
  ([`0f54422`](https://github.com/Promptly-Technologies-LLC/etielle/commit/0f54422211abe9080533cefd1a8b78eb20aab4ba))

### Features

- Support Supabase-generated PKs
  ([`6d4c76c`](https://github.com/Promptly-Technologies-LLC/etielle/commit/6d4c76ca95e47784593daac42adfb254ccf68f42))


## v3.1.0 (2025-12-02)

### Documentation

- Add upsert_on parameter and Supabase to README
  ([`4d23be0`](https://github.com/Promptly-Technologies-LLC/etielle/commit/4d23be037d8fe49d36a69921a93e24c2451b4eaf))

### Features

- **supabase**: Support per-table upsert conflict columns
  ([`ab11f85`](https://github.com/Promptly-Technologies-LLC/etielle/commit/ab11f855f4f70e2c933aad52a6142b80e5167f62))


## v3.0.0 (2025-12-02)

### Refactoring

- Remove legacy SQLAlchemy/SQLModel adapters
  ([`33fd86c`](https://github.com/Promptly-Technologies-LLC/etielle/commit/33fd86c328aea3498bc9338384454f93108aa668))

### Testing

- Add comprehensive test coverage for documented behaviors
  ([`1b5e138`](https://github.com/Promptly-Technologies-LLC/etielle/commit/1b5e13849b04451fe791bb510d3909d0c04c4d46))

### Breaking Changes

- The legacy bind_and_flush adapters have been removed. Use the fluent API (.load(session).run())
  instead.


## v2.6.0 (2025-12-02)

### Chores

- Add supabase/ to .gitignore
  ([`15a76fc`](https://github.com/Promptly-Technologies-LLC/etielle/commit/15a76fc4917f595057afb531561a4c7e36ca82b7))

### Documentation

- Add CONTRIBUTING.md with dev setup and Supabase test instructions
  ([`3d6525a`](https://github.com/Promptly-Technologies-LLC/etielle/commit/3d6525a97a963dfc2ab680453f1ef6b6be28b2ce))

### Features

- **adapter**: Add Supabase adapter
  ([`8c62339`](https://github.com/Promptly-Technologies-LLC/etielle/commit/8c623391788249245c2dbb8efb36dae1944fbe71))


## v2.5.0 (2025-12-01)

### Documentation

- Add apply transform to documentation
  ([`487db7a`](https://github.com/Promptly-Technologies-LLC/etielle/commit/487db7afc47774cb8383a448d2905b6e42424846))

### Features

- Add apply transform for type coercion and function application
  ([`136badf`](https://github.com/Promptly-Technologies-LLC/etielle/commit/136badf2d470abf48eebb79c589f6415b53f99be))


## v2.4.0 (2025-12-01)

### Bug Fixes

- Join_on fields are now persisted instead of excluded
  ([`6b7b895`](https://github.com/Promptly-Technologies-LLC/etielle/commit/6b7b895f9f477cf6847eb3c906014706732ef7c4))

- Proper flush ordering for NOT NULL FK constraints
  ([`ba23476`](https://github.com/Promptly-Technologies-LLC/etielle/commit/ba23476bf97075ce424c0cbecc73b7b0e59f3ade))

- Remove fields() proxy and undeprecate field_of
  ([`6eed551`](https://github.com/Promptly-Technologies-LLC/etielle/commit/6eed551aa8d98c3b43a1550a8d8099d830ad9c33))

- Resolve spurious iteration bug
  ([`50af234`](https://github.com/Promptly-Technologies-LLC/etielle/commit/50af234e55a9dcd5a9113716c213ab56b80202d3))

- Resolve type errors
  ([`eb30b0f`](https://github.com/Promptly-Technologies-LLC/etielle/commit/eb30b0faa4eabbbb822264542bb6241ca3615ddc))

- Singleton mapping without explicit join key now persists correctly
  ([`965ffd2`](https://github.com/Promptly-Technologies-LLC/etielle/commit/965ffd2c2b3372729befba7eeef23f5ecbdf2395))

- Singleton parents can now be linked by children
  ([`cb19d14`](https://github.com/Promptly-Technologies-LLC/etielle/commit/cb19d14a2c6c3cb0a86747b14c12e3354f84fe22))

### Documentation

- Add fluent API documentation to README
  ([`ae9078a`](https://github.com/Promptly-Technologies-LLC/etielle/commit/ae9078a483cfdf2aa1f521079911395209db5887))

- Full documentation rewrite
  ([`c2b553c`](https://github.com/Promptly-Technologies-LLC/etielle/commit/c2b553c5489dfdf748e9f2c8630b94a6fbd434da))

- Update docs to reflect architecture changes
  ([`b3744e7`](https://github.com/Promptly-Technologies-LLC/etielle/commit/b3744e78d69c607c1a0fada534b9e459bf4cd03f))

### Features

- Add _build_dependency_graph to PipelineBuilder
  ([`682eef7`](https://github.com/Promptly-Technologies-LLC/etielle/commit/682eef71c628363e393ac8459094ce9ef508959d))

- Add _get_linkable_fields to extract linking fields from pipeline
  ([`f5e02d4`](https://github.com/Promptly-Technologies-LLC/etielle/commit/f5e02d47d8d1a069d584a0af411a98894eaef34e))

- Add bind_many_to_one_via_index for secondary index lookup
  ([`4ed9ab1`](https://github.com/Promptly-Technologies-LLC/etielle/commit/4ed9ab1192e528064a0cd4158a97768f2260a2e1))

- Add indices field to MappingResult for secondary indices
  ([`cbd05c7`](https://github.com/Promptly-Technologies-LLC/etielle/commit/cbd05c7b0cd0070ff841c7d4b5962ba3797116cc))

- Add topological_sort utility for dependency ordering
  ([`42b416e`](https://github.com/Promptly-Technologies-LLC/etielle/commit/42b416efa953f45c20f5d1689a37b036d857e376))

- Build secondary indices for linkable fields during instance creation
  ([`4cdf311`](https://github.com/Promptly-Technologies-LLC/etielle/commit/4cdf31178bfa0df1f0bbaa984a41b8df91949e12))

- Decouple join keys from relationship linking
  ([`4295408`](https://github.com/Promptly-Technologies-LLC/etielle/commit/429540876e5e7e7ca67c24ed352a84c7a5c8dff4))

- Export fluent API from package root
  ([`4b3b4aa`](https://github.com/Promptly-Technologies-LLC/etielle/commit/4b3b4aabda6f652df11d9620565e175d1d93d0c3))

- Implement dependency-ordered flushing for auto-generated PKs
  ([`06864f8`](https://github.com/Promptly-Technologies-LLC/etielle/commit/06864f885d5b784fa8cdc63d4d34f3b5f7627161))

- **fluent**: Add @transform decorator for custom transforms
  ([`fada2b7`](https://github.com/Promptly-Technologies-LLC/etielle/commit/fada2b7fd3ddbaf3a57b23d160197e6bd2c34321))

- **fluent**: Add automatic model type detection for builders
  ([`a64ff0d`](https://github.com/Promptly-Technologies-LLC/etielle/commit/a64ff0d0d25d9ac692ea306a3a6714c90df5db20))

- **fluent**: Add database persistence with load().run()
  ([`e14cd8c`](https://github.com/Promptly-Technologies-LLC/etielle/commit/e14cd8cb7c98421ea6a7ccec80ff03f408c9d600))

- **fluent**: Add each() iteration marker
  ([`8a817a0`](https://github.com/Promptly-Technologies-LLC/etielle/commit/8a817a054ec366e01b8ea34a4bcd4bdbe09d921b))

- **fluent**: Add Field dataclass for persisted fields
  ([`9e382c1`](https://github.com/Promptly-Technologies-LLC/etielle/commit/9e382c16720c83c6f7c6897efc04c219f0151e80))

- **fluent**: Add FieldUnion type alias
  ([`9f5ae39`](https://github.com/Promptly-Technologies-LLC/etielle/commit/9f5ae39a7834760d913aff5ca87d06629db485f5))

- **fluent**: Add goto() navigation method
  ([`9b3e14a`](https://github.com/Promptly-Technologies-LLC/etielle/commit/9b3e14a803e28ede170bc3c34a102fa10e4db05c))

- **fluent**: Add goto_root() navigation method
  ([`4c52b32`](https://github.com/Promptly-Technologies-LLC/etielle/commit/4c52b321bcf1142faddb8e6b249737ae44044ac8))

- **fluent**: Add link_to() relationship method
  ([`bc67613`](https://github.com/Promptly-Technologies-LLC/etielle/commit/bc67613dee17dc85c9f1cfcf9803511ff46fb412))

- **fluent**: Add load() session configuration method
  ([`0f6edf8`](https://github.com/Promptly-Technologies-LLC/etielle/commit/0f6edf89af6f091635dd83b8e9d3c7288fc23073))

- **fluent**: Add map_to() emission method
  ([`09b1e6a`](https://github.com/Promptly-Technologies-LLC/etielle/commit/09b1e6a5a9188fadf750d864acae08fc9a84974a))

- **fluent**: Add multiple root support in run()
  ([`3fc47ee`](https://github.com/Promptly-Technologies-LLC/etielle/commit/3fc47eeee6e3888e51a2a6325b27a3f477a8f7d4))

- **fluent**: Add node() transform
  ([`5a5d5e1`](https://github.com/Promptly-Technologies-LLC/etielle/commit/5a5d5e164b0979a0641c9466591be248156f4d7e))

- **fluent**: Add parent_index() transform
  ([`0620c99`](https://github.com/Promptly-Technologies-LLC/etielle/commit/0620c99326a5139bdfe8d12ab955c819cfff175e))

- **fluent**: Add PipelineBuilder skeleton and etl() entry point
  ([`e5d1c0d`](https://github.com/Promptly-Technologies-LLC/etielle/commit/e5d1c0d3d50f55b4aa5c459a4b17420e9953af3b))

- **fluent**: Add PipelineResult with _TablesProxy
  ([`2ed4844`](https://github.com/Promptly-Technologies-LLC/etielle/commit/2ed48444d539dfd972a916d51f59b520e5bf0d7f))

- **fluent**: Add relationship binding in run()
  ([`da4f6a2`](https://github.com/Promptly-Technologies-LLC/etielle/commit/da4f6a2ce855f28747fe53bf7882a623072fc1bd))

- **fluent**: Add run() execution method with basic extraction
  ([`dd8a627`](https://github.com/Promptly-Technologies-LLC/etielle/commit/dd8a627e2ab7d8a4ddaba2865dcb70774dd8fef7))

- **fluent**: Add TempField dataclass for join-only fields
  ([`5cd86b8`](https://github.com/Promptly-Technologies-LLC/etielle/commit/5cd86b8f7b196d9fd95430e640ef0a78544bb331))

### Testing

- **fluent**: Add error handling mode tests
  ([`4b04968`](https://github.com/Promptly-Technologies-LLC/etielle/commit/4b0496897109f6233b2ac4e3ae55ed9a9fb112b8))

- **fluent**: Add tests for row merging with join_on
  ([`3e0382b`](https://github.com/Promptly-Technologies-LLC/etielle/commit/3e0382b524c08133a930fda88de92f58d3c88c12))


## v2.3.2 (2025-11-29)

### Bug Fixes

- Transform from Protocol to TypeAlias
  ([`3f8a377`](https://github.com/Promptly-Technologies-LLC/etielle/commit/3f8a37766503dc93e78b8696903bb5d859e026b9))


## v2.3.1 (2025-11-29)

### Bug Fixes

- Export types
  ([`e8d66d9`](https://github.com/Promptly-Technologies-LLC/etielle/commit/e8d66d9f174eb6193e69e4f52dfca86f0b1d57ad))


## v2.3.0 (2025-11-29)

### Documentation

- More logical sequence, better examples
  ([`16f9b32`](https://github.com/Promptly-Technologies-LLC/etielle/commit/16f9b320711e1873286f935cb6c27e82f421de1d))

- Resolve some link resolution issues
  ([`0663cbd`](https://github.com/Promptly-Technologies-LLC/etielle/commit/0663cbdbf27fc3214b15653356bcf717173fba8b))

### Features

- New `fields` selector for class instances
  ([`0d0dc11`](https://github.com/Promptly-Technologies-LLC/etielle/commit/0d0dc11260cc7c4ef426fb8ff387ea3e333f231d))


## v2.2.1 (2025-11-25)

### Bug Fixes

- Support >1 spec per child table
  ([`57ee01e`](https://github.com/Promptly-Technologies-LLC/etielle/commit/57ee01ea40d50350278b91337af3136831f56662))

### Chores

- Lint code
  ([`9f4d3c8`](https://github.com/Promptly-Technologies-LLC/etielle/commit/9f4d3c89bfc2f39c1fc2d8e6bfe748aa6438f79c))

### Documentation

- Add comprehensive introduction to ETL documentation
  ([`69e8029`](https://github.com/Promptly-Technologies-LLC/etielle/commit/69e8029cdc441ce330e4f36607bc624c1ca313d3))

- Introduction to ETL
  ([`90e0fd1`](https://github.com/Promptly-Technologies-LLC/etielle/commit/90e0fd1a5d9412562c10b444d7274b56ea3f887b))

- Refine ETL concept mapping in introduction
  ([`bbdb9a7`](https://github.com/Promptly-Technologies-LLC/etielle/commit/bbdb9a7b36c25b883c4e440782559f24d6042592))


## v2.2.0 (2025-10-23)

### Chores

- Remove lockfile from version control
  ([`0229547`](https://github.com/Promptly-Technologies-LLC/etielle/commit/022954758ee7e72cb42909bb8bdab8ebff030a6e))

### Features

- New ConstructorBuilder for ORMs
  ([`977b0a0`](https://github.com/Promptly-Technologies-LLC/etielle/commit/977b0a0e866cadaae5a7cb2ef343dcfc117c2f84))


## v2.1.0 (2025-10-20)

### Documentation

- Clarify field selector docs
  ([`cb90959`](https://github.com/Promptly-Technologies-LLC/etielle/commit/cb90959941ab9ede524e33f4293570c7b72f7cfa))

- Cross-linking, user-friendliness
  ([`acb8b87`](https://github.com/Promptly-Technologies-LLC/etielle/commit/acb8b871912523926cdf0816069d62e55125b8fb))

- Improve clarity of README
  ([`dbc75c2`](https://github.com/Promptly-Technologies-LLC/etielle/commit/dbc75c2e24a1fb111a04b18b3b3b203ba5feb477))

- More detailed emission guide
  ([`782ab62`](https://github.com/Promptly-Technologies-LLC/etielle/commit/782ab6246c3518f8e543a408f335aa7722467d2b))

### Features

- Consolidate/enhance adapter documentation
  ([`e6df5ed`](https://github.com/Promptly-Technologies-LLC/etielle/commit/e6df5edc5fa283c7411d9f909841b07ed60cf431))


## v2.0.0 (2025-10-19)

### Bug Fixes

- Make release workflow ff after publish
  ([`506d3c8`](https://github.com/Promptly-Technologies-LLC/etielle/commit/506d3c86390bbac20cb36ef7fcbbd0b6d97a3b30))

### Documentation

- Add backticks around code
  ([`e251f3f`](https://github.com/Promptly-Technologies-LLC/etielle/commit/e251f3f5c24b6f9965b20a3fc006224e60b0fab0))

- All documentation code runs
  ([`61dbb24`](https://github.com/Promptly-Technologies-LLC/etielle/commit/61dbb245bdff603dd37b9240e7e0ec74a2241229))

- Documentation website
  ([`8019c14`](https://github.com/Promptly-Technologies-LLC/etielle/commit/8019c1489f710f70fcafdf08c723a1a9929d840d))

- Example code triggers errors
  ([`b12093e`](https://github.com/Promptly-Technologies-LLC/etielle/commit/b12093ef19389e5663785a460f082aa8c2e861fd))

- Fix header text color
  ([`69ab9d4`](https://github.com/Promptly-Technologies-LLC/etielle/commit/69ab9d44e73725c5482e9132259aba836a86b88a))

- Print mapping result
  ([`1ef1164`](https://github.com/Promptly-Technologies-LLC/etielle/commit/1ef1164a844a2045163be515175da3a3b61de645))

### Features

- Simplify iteration API
  ([`25e1c17`](https://github.com/Promptly-Technologies-LLC/etielle/commit/25e1c177ef497880f912b5c9770e01718298138d))


## v1.4.0 (2025-10-15)

### Features

- Sqlalchemy adapter
  ([`3ac07a1`](https://github.com/Promptly-Technologies-LLC/etielle/commit/3ac07a135d487d842552788d032dcbf0cf86757e))


## v1.3.0 (2025-10-14)

### Features

- Mutation-based emit
  ([`21dbbe6`](https://github.com/Promptly-Technologies-LLC/etielle/commit/21dbbe60c4feae493eec00c675dc0f698069d969))


## v1.2.0 (2025-10-14)

### Features

- Error reporting
  ([`10657ec`](https://github.com/Promptly-Technologies-LLC/etielle/commit/10657ec161b6f09c8b95e5f4ae952d8f2bd70f39))

- Instance emission adapter
  ([`4c02de0`](https://github.com/Promptly-Technologies-LLC/etielle/commit/4c02de0a87fae2309f8c4e3e0662452c31f338ca))


## v1.1.0 (2025-10-14)

### Features

- Implement field selectors API
  ([`e099dbe`](https://github.com/Promptly-Technologies-LLC/etielle/commit/e099dbe9dda503fc745370dcfe9850918641ac35))


## v1.0.6 (2025-10-14)

### Bug Fixes

- Configure pyproject version stamping
  ([`49c4e07`](https://github.com/Promptly-Technologies-LLC/etielle/commit/49c4e07af4cf72c446c3847399abe776e515ab85))


## v1.0.5 (2025-10-14)

### Bug Fixes

- Make release dependent on test
  ([`b377958`](https://github.com/Promptly-Technologies-LLC/etielle/commit/b37795886a41674c677bc13e2df676fe42effc6f))


## v1.0.4 (2025-10-14)

### Bug Fixes

- Correctly use PyPi env for publish job
  ([`2dd6cea`](https://github.com/Promptly-Technologies-LLC/etielle/commit/2dd6cea8ff3fd99a5ac64d627721bcdac394750a))


## v1.0.3 (2025-10-14)

### Bug Fixes

- Run build in PSR container
  ([`6c359c6`](https://github.com/Promptly-Technologies-LLC/etielle/commit/6c359c6bc281118068fe1b41a6f5e8db4d8cad87))


## v1.0.2 (2025-10-14)

### Bug Fixes

- Restore build step
  ([`4250642`](https://github.com/Promptly-Technologies-LLC/etielle/commit/42506424365d86a321acc044e0363840da0dc6f3))


## v1.0.1 (2025-10-14)

### Bug Fixes

- Gate artifact upload
  ([`5e5bda1`](https://github.com/Promptly-Technologies-LLC/etielle/commit/5e5bda1ee36397b3913ab3a1cbc58b28e5d3f458))


## v1.0.0 (2025-10-14)

### Bug Fixes

- Actions-compliant root path
  ([`251d553`](https://github.com/Promptly-Technologies-LLC/etielle/commit/251d5530f61aaef988f0ed23bcf312e85f3a822c))

- Add missing semantic release config
  ([`f6235d4`](https://github.com/Promptly-Technologies-LLC/etielle/commit/f6235d449d76a8ae9d6788d494f08d44a612fd10))

- Semantic release version mismatch
  ([`3dbf9a0`](https://github.com/Promptly-Technologies-LLC/etielle/commit/3dbf9a0559a47e59efbf23d9ed4a935422283647))

- Use semantic release's built-in committer
  ([`6681c31`](https://github.com/Promptly-Technologies-LLC/etielle/commit/6681c31c30c29f835998ec1a77d39d752e64c52a))
