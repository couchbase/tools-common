#!/usr/bin/env groovy

import hudson.model.Result
import hudson.model.Run
import jenkins.model.CauseOfInterruption.UserInterruption

pipeline {
    agent { label "linux&&master&&!aarch64" }

    environment {
        GO_TARBALL_URL = "https://golang.org/dl/go1.24.11.linux-amd64.tar.gz"

        GOROOT = "${WORKSPACE}/go"
        GOBIN = "${GOROOT}/bin"
        PATH="${PATH}:${GOBIN}:${WORKSPACE}/bin"

        GOLANGCI_LINT_VERSION = "v1.62.2"

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

                timeout(time: 5, unit: "MINUTES") {
                    // Install Golang locally
                    sh "wget -q -O- ${GO_TARBALL_URL} | tar xz"

                    // get golangci-lint binary
                    sh "curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/${GOLANGCI_LINT_VERSION}/install.sh | sh -s -- -b ${GOBIN} ${GOLANGCI_LINT_VERSION}"
                    sh "golangci-lint --version"

                    // A clone of 'jq' written in Go (required for merging coverage files)
                    sh "go install github.com/itchyny/gojq/cmd/gojq@latest"

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
                timeout(time: 10, unit: "MINUTES") {
                    dir("${PROJECT}") {
                        sh "golangci-lint cache clean"

                        sh "./scripts/cv/lint.sh"
                    }
                }
            }
        }

        stage("Test") {
            steps {
                // Create somewhere to store our coverage/test reports
                sh "mkdir -p reports"

                timeout(time: 15, unit: "MINUTES") {
                    dir("${PROJECT}") {
                        // Run unit testing with coverage and place output in 'reports'.
                        //
                        // - 'reports/tests.xml'
                        // - 'reports/coverage.xml'
                        sh "./scripts/cv/test.sh ${WORKSPACE}/reports"
                    }
                }
            }
        }

        stage("Benchmark") {
            steps {
                timeout(time: 15, unit: "MINUTES") {
                    dir("${PROJECT}") {
                        // Run the benchmarks without running any tests
                        sh "./scripts/cv/benchmark.sh"
                    }
                }
            }
        }
    }

    post {
        always {
            // Post the test results
            junit allowEmptyResults: true, testResults: "reports/tests.xml"

            // Post the test coverage
            recordCoverage(
                tools: [[parser: 'COBERTURA', pattern: 'reports/coverage.xml']],
                qualityGates: [[threshold: 30.0, metric: 'LINE', baseline: 'PROJECT']],
                sourceDirectories: [[path: "tools-common/"]],
                sourceCodeRetention: "LAST_BUILD",
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
