pipeline {
    agent any
    environment {
        github = credentials('4145361a-82d9-4899-8224-6e9071be7c45')
    }
    stages {
        stage('Pull') {
            steps {
                git branch: 'master',
                credentialsId: '4145361a-82d9-4899-8224-6e9071be7c45',
                url: 'https://github.com/LightshieldDotDev/RiotApi'
            }
        }
        stage('Select Changes') {
            environment {
            def PROXY_CHANGED = sh(
                    script: 'git diff --quiet HEAD master -- proxy || echo changed',
                    returnStdout: true
                ).trim()
            def LEAGUE_UPDATER_CHANGED = sh(
                    script: 'git diff --quiet HEAD master -- league_updater || echo changed',
                    returnStdout: true
                ).trim()
            def SUMMONER_ID_UPDATER_CHANGED = sh(
                    script: 'git diff --quiet HEAD master -- summoner_id_updater || echo changed',
                    returnStdout: true
                ).trim()
            def MATCH_HISTORY_UPDATER_CHANGED = sh(
                    script: 'git diff --quiet HEAD master -- match_history_updater || echo changed',
                    returnStdout: true
                ).trim()
            }
            steps {
                script {
                    if (!env.PROXY_CHANGED) {
                        sh 'echo No changes for proxy found.'
                    } else {
                        sh 'echo Recreating proxy.'
                    }
                    if (!env.LEAGUE_UPDATER_CHANGED) {
                        sh 'echo No changes for league_updater found.'
                    } else {
                        sh 'echo Recreating league_updater.'
                    }
                    if (!env.SUMMONER_ID_UPDATER_CHANGED) {
                        sh 'echo No changes for summoner_id_updater found.'
                    } else {
                        sh 'echo Recreating summoner_id_updater.'
                    }
                    if (!env.MATCH_HISTORY_UPDATER_CHANGED) {
                        sh 'echo No changes for match_history_updater found.'
                    } else {
                        sh 'echo Recreating match_history_updater.'
                    }
                }
            }
        }
        stage('Clean Code Tests') {
            steps {
                dir('proxy') {
                    script {
                        if (!env.PROXY_CHANGED) {
                            sh 'tox'
                        }
                    }
                }
                dir('league_updater') {
                    script {
                        if (!env.LEAGUE_UPDATER_CHANGED) {
                            sh 'tox'
                        }
                    }
                }
                dir('summoner_id_updater') {
                    script {
                        if (!env.SUMMONER_ID_UPDATER_CHANGED) {
                            sh 'tox'
                        }
                    }
                }
                dir('match_history_updater') {
                    script {
                        if (!env.MATCH_HISTORY_UPDATER_CHANGED) {
                            sh 'tox'
                        }
                    }
                }
            }
        }
    }
}