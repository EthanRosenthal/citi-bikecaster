STAGE   ?= dev
PROFILE ?= rd
REGION  ?= us-east-1
BUCKET  ?= insulator-citi-bikecaster
AWS      = aws --profile $(PROFILE) --region $(REGION)
STACK    = $(if $(filter prod,$(STAGE)),citibike-v2,citibike-v2-$(STAGE))
NAME     = $(if $(filter prod,$(STAGE)),citibike,citibike-$(STAGE))
# Extra CloudFormation parameters, e.g. PARAMS="SchedulesEnabled=false"
PARAMS  ?=

.PHONY: test test-live layer package deploy lifecycle invoke logs clean

test:
	uv run pytest -m "not live"

test-live:
	uv run pytest -m live

# pyarrow for the Lambda runtime (python3.14 / arm64 / AL2023), pinned by uv.lock.
layer: build/layer/.built
build/layer/.built: uv.lock
	rm -rf build/layer && mkdir -p build
	uv export --frozen --no-dev --no-emit-project --no-hashes -o build/requirements.txt
	uv pip install --quiet --target build/layer/python -r build/requirements.txt \
		--python-platform aarch64-manylinux_2_34 --python-version 3.14 --only-binary=:all:
	rm -rf build/layer/python/pyarrow/include build/layer/python/pyarrow/tests
	find build/layer -name '__pycache__' -prune -exec rm -rf {} +
	find build/layer \( -name '*.pyx' -o -name '*.pxd' -o -name '*.pxi' \) -delete
	touch $@

package: layer
	find src -name '__pycache__' -prune -exec rm -rf {} +
	$(AWS) cloudformation package --template-file template.yaml \
		--s3-bucket $(BUCKET) --s3-prefix deploy/$(STACK) \
		--output-template-file build/packaged.yaml

deploy: package
	$(AWS) cloudformation deploy --template-file build/packaged.yaml --stack-name $(STACK) \
		--capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND --no-fail-on-empty-changeset \
		--parameter-overrides Stage=$(STAGE) $(PARAMS)

# The bucket is not owned by the stack, so its lifecycle rules live here.
lifecycle:
	$(AWS) s3api put-bucket-lifecycle-configuration --bucket $(BUCKET) \
		--lifecycle-configuration file://infra/lifecycle.json

# make invoke FN=compact-daily EVENT='{"date": "2026-09-23"}'
FN    ?= station-status
EVENT ?= {}
invoke:
	$(AWS) lambda invoke --function-name $(NAME)-$(FN) --cli-binary-format raw-in-base64-out \
		--cli-read-timeout 0 --payload '$(EVENT)' /dev/stdout

logs:
	$(AWS) logs tail --follow $$($(AWS) lambda get-function-configuration --function-name $(NAME)-$(FN) \
		--query LoggingConfig.LogGroup --output text)

clean:
	rm -rf build
