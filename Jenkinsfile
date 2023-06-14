#!/usr/bin/env groovy

import hudson.model.Result
import hudson.model.Run
import jenkins.model.CauseOfInterruption.UserInterruption

pipeline {
    agent { label "linux&&master" }

    environment {
        GO_TARBALL_URL = "https://golang.org/dl/go1.18.6.linux-amd64.tar.gz"

        GOROOT = "${WORKSPACE}/go"
        GOBIN = "${GOROOT}/bin"
        PATH="${PATH}:${GOBIN}:${WORKSPACE}/bin"

        GOLANGCI_LINT_VERSION = "v1.47.3"

        PROJECT = "${WORKSPACE}/tools-common"
    }

    stages {
        stage("Setup") {
            steps {
                script {
                    // Configure Gerrit Trigger
                    properties([pipelineTriggers([
                        gerrit(
                            serverName: "review.couchbase.org",
                            gerritProjects: [
                                [
                                    compareType: "PLAIN", disableStrictForbiddenFileVerification: false,
                                    pattern: "tools-common", branches: [[ compareType: "PLAIN", pattern: "master" ]]
                                ],
                            ],
                            triggerOnEvents: [
                                commentAddedContains(commentAddedCommentContains: "reverify"),
                                draftPublished(),
                                patchsetCreated(excludeNoCodeChange: true)
                            ]
                        )
                    ])])
                }

                slackSend(
                    channel: "#tooling-cv",
                    color: "good",
                    message: "Build for '<${GERRIT_CHANGE_URL}|${GERRIT_CHANGE_SUBJECT}>' by '${GERRIT_CHANGE_OWNER_NAME}' started (${env.BUILD_URL})"
                )

                timeout(time: 5, unit: "MINUTES") {
                    // Install Golang locally
                    sh "wget -q -O- ${GO_TARBALL_URL} | tar xz"

                    // get golangci-lint binary
                    sh "curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/${GOLANGCI_LINT_VERSION}/install.sh | sh -s -- -b ${GOBIN} ${GOLANGCI_LINT_VERSION}"
                    sh "golangci-lint --version"

                    // Unit test reporting
                    sh "go install github.com/jstemmer/go-junit-report@latest"

                    // Coverage reporting
                    sh "go install github.com/axw/gocov/gocov@latest"
                    sh "go install github.com/AlekSi/gocov-xml@latest"

                    // clone project
                    sh "git clone git@github.com:couchbase/tools-common.git"

                    // Fetch the commit we are testing
                    dir("${PROJECT}") {
                        sh "git fetch ssh://buildbot@review.couchbase.org:29418/tools-common ${GERRIT_REFSPEC}"
                        sh "git checkout FETCH_HEAD"
                    }
                }
            }
        }

        stage("Lint") {
            steps {
                timeout(time: 5, unit: "MINUTES") {
                    dir("${PROJECT}") {
                        sh "golangci-lint run --timeout 5m"
                    }
                }
            }
        }

        stage("Test") {
            steps {
                // Create somewhere to store our coverage/test reports
                sh "mkdir -p reports"

                dir("${PROJECT}") {
                    // Clean the Go test cache
                    sh "go clean -testcache"

                    // Run the unit testing
                    sh "2>&1 go test -v -timeout=15m -count=1 -coverprofile=coverage.out ./... | tee ${WORKSPACE}/reports/test.raw"

                    // Convert the test output into valid 'junit' xml
                    sh "cat ${WORKSPACE}/reports/test.raw | go-junit-report > ${WORKSPACE}/reports/test.xml"

                    // Convert the coverage report into valid 'cobertura' xml
                    sh "gocov convert coverage.out | gocov-xml > ${WORKSPACE}/reports/coverage.xml"
                }
            }
        }

        stage("Benchmark") {
            steps {
                dir("${PROJECT}") {
                    // Run the benchmarks without running any tests by setting '-run='^$'
                    sh "go test -timeout=15m -count=1 -run='^\044' -bench=Benchmark ./..."
                }
            }
        }
    }

    post {
        always {
            // Post the test results
            junit allowEmptyResults: true, testResults: "reports/test.xml"

            // Post the test coverage
            cobertura autoUpdateStability: false, autoUpdateHealth: false, onlyStable: false, coberturaReportFile: "reports/coverage.xml", conditionalCoverageTargets: "70, 10, 30", failNoReports: false, failUnhealthy: true, failUnstable: true, lineCoverageTargets: "70, 10, 30", methodCoverageTargets: "70, 10, 30", maxNumberOfBuilds: 0, sourceEncoding: "ASCII", zoomCoverageChart: false
        }

        success {
            slackSend(
                channel: "#tooling-cv",
                color: "good",
                message: "Build for '<${GERRIT_CHANGE_URL}|${GERRIT_CHANGE_SUBJECT}>' by '${GERRIT_CHANGE_OWNER_NAME}' succeeded (${env.BUILD_URL})"
            )
        }

        unstable {
            slackSend(
                channel: "#tooling-cv",
                color: "bad",
                message: "Build for '<${GERRIT_CHANGE_URL}|${GERRIT_CHANGE_SUBJECT}>' by '${GERRIT_CHANGE_OWNER_NAME}' is unstable (${env.BUILD_URL})"
            )
        }

        failure {
            slackSend(
                channel: "#tooling-cv",
                color: "bad",
                message: "Build for '<${GERRIT_CHANGE_URL}|${GERRIT_CHANGE_SUBJECT}>' by '${GERRIT_CHANGE_OWNER_NAME}' failed (${env.BUILD_URL})"
            )
        }

        aborted {
            slackSend(
                channel: "#tooling-cv",
                color: "bad",
                message: "Build for '<${GERRIT_CHANGE_URL}|${GERRIT_CHANGE_SUBJECT}>' by '${GERRIT_CHANGE_OWNER_NAME}' aborted (${env.BUILD_URL})"
            )
        }

        cleanup {
            // We don't need the build cache interfering with any subsequent builds
            sh "go clean --cache --testcache"

            // Remove the workspace
            deleteDir()
        }
    }
}
