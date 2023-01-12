/**
* JetBrains Space Automation
* This Kotlin-script file lets you automate build activities
* For more info, see https://www.jetbrains.com/help/space/automation.html
*/

job("Qodana") {
    // For example: "jetbrains/qodana-jvm-community:2021.3"
    container("jetbrains/qodana-jvm-community:2021.3") {
        // https://www.jetbrains.com/help/space/secrets-and-parameters.html
        env["QODANA_TOKEN"] = Secrets("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJvcmdhbml6YXRpb24iOiJ6eFhEeiIsInByb2plY3QiOiJBWU9udyIsInRva2VuIjoiQWdNUTMifQ.97oAguPHm6lQ9E0uD-PSyW-quRI8KhgJDVnt3Ix7Oio")
        shellScript {
            content = """
               QODANA_REPO_URL="${'$'}JB_SPACE_API_URL/p/${'$'}JB_SPACE_PROJECT_KEY/repositories/${'$'}JB_SPACE_GIT_REPOSITORY_NAME" \
               QODANA_BRANCH=${'$'}JB_SPACE_GIT_BRANCH \
               QODANA_REVISION=${'$'}JB_SPACE_GIT_REVISION \
               QODANA_ENV="space" \
               qodana
            """.trimIndent()
        }
    }
}
