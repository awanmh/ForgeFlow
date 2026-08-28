pipeline {
    agent any

    environment {
        GO111MODULE = 'on'
        CGO_ENABLED = '1'
        ENV = 'test'
        LOG_LEVEL = 'info'
    }

    options {
        timeout(time: 15, unit: 'MINUTES')
        buildDiscarder(logRotator(numToKeepStr: '10', artifactNumToKeepStr: '5'))
        disableConcurrentBuilds()
        timestamps()
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Verify Environment & Dependencies') {
            steps {
                sh '''
                    echo "=== Go Toolchain Info ==="
                    go version || (echo "Go toolchain not found in PATH" && exit 1)
                    go env
                    echo "=== Verifying Go Modules ==="
                    go mod verify
                    go mod download
                '''
            }
        }

        stage('Static Analysis & Vet') {
            steps {
                sh '''
                    echo "=== Running go vet ==="
                    go vet ./...
                '''
            }
        }

        stage('Unit & Race Detector Tests') {
            steps {
                sh '''
                    echo "=== Running Go Tests with Race Detection & Coverage ==="
                    go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...
                '''
            }
            post {
                always {
                    archiveArtifacts artifacts: 'coverage.txt', allowEmptyArchive: true
                }
            }
        }

        stage('Build Service Binaries') {
            steps {
                sh '''
                    echo "=== Building Service Binaries ==="
                    make build
                    ls -la bin/
                '''
            }
        }

        stage('Docker Configuration Validation') {
            steps {
                sh '''
                    echo "=== Validating Docker Compose Configuration ==="
                    docker compose config > /dev/null 2>&1 || docker-compose config || true
                '''
            }
        }
    }

    post {
        success {
            echo "✅ ForgeFlow CI Pipeline completed successfully!"
        }
        failure {
            echo "❌ ForgeFlow CI Pipeline failed. Check console output for details."
        }
        always {
            cleanWs(cleanWhenNotBuilt: false,
                    deleteDirs: true,
                    notFailBuild: true)
        }
    }
}
