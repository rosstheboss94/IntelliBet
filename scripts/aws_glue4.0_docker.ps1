param (
	[string]$DOT_AWS = "",
	[string]$WORKSPACE_LOCATION = "",
	[string]$AWS_PROFILE = ""
)

$configFilePath = "./config.json"

$config = Get-Content -Raw -Path $configFilePath | ConvertFrom-Json

if ($DOT_AWS -eq "") {
	$DOT_AWS = $config.DOT_AWS
}

if ($WORKSPACE_LOCATION -eq "") {
	$WORKSPACE_LOCATION = $config.WORKSPACE_LOCATION
}

if ($AWS_PROFILE -eq "") {
	$AWS_PROFILE = $config.AWS_PROFILE
}

docker run -it -v ${DOT_AWS}:/home/glue_user/.aws -v ${WORKSPACE_LOCATION}:/home/glue_user/workspace/ -e AWS_PROFILE=${AWS_PROFILE} -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 --name glue_pyspark amazon/aws-glue-libs:glue_libs_4.0.0_image_01
