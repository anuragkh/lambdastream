role_arn = arn:aws:iam::123456789012:role/RoleName

package: clean
	pip3 install external/jiffy --target packages
	LAMBDA_INSTALL=true pip3 install . --target packages
	cd packages && zip -r9 ../function.zip .

create: package
	aws lambda create-function --function-name lambda_stream_operator \
		--runtime python3.7 \
		--role $(role_arn) \
		--handler lambdastream.aws.operator_handler \
		--zip-file fileb://function.zip

update: package
	aws lambda update-function-code --function-name lambda_stream_operator \
		--zip-file fileb://function.zip

clean:
	rm -rf function.zip packages
