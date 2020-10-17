/* eslint-disable no-prototype-builtins */
'use strict';

const uuid = require('uuid/v4');
const nock = require('nock');
const { expect } = require('chai');

const statsGenerator = require('./helpers/statsGenerator');
const reportsRequestCreator = require('./helpers/requestCreator');
const jobRequestCreator = require('../jobs/helpers/requestCreator');
const testsRequestCreator = require('../tests/helpers/requestCreator');
const configRequestCreator = require('../configManager/helpers/requestCreator');
const constants = require('../../../src/reports/utils/constants');
const kubernetesConfig = require('../../../src/config/kubernetesConfig');
const basicTest = require('../../testExamples/Basic_test');
const { KUBERNETES } = require('../../../src/common/consts');
const mailhogHelper = require('./mailhog/mailhogHelper');


const headers = { 'Content-Type': 'application/json' };

const jobPlatform = process.env.JOB_PLATFORM;
// I did this to save an indentation level
(jobPlatform.toUpperCase() === KUBERNETES ? describe : describe.skip)('Reports integration tests', function() {
    before('Init requestCreators', async function() {
        await reportsRequestCreator.init();
        await testsRequestCreator.init();
        await jobRequestCreator.init();
        await configRequestCreator.init();

        const requestBody = basicTest;
        const response = await testsRequestCreator.createTest(requestBody, {});
        should(response.statusCode).eql(201);
        should(response.body).have.key('id');
        testId = response.body.id;

        const responseA = await testsRequestCreator.createTest(requestBody, {});
        should(responseA.statusCode).eql(201);
        should(responseA.body).have.key('id');
        testIdA = responseA.body.id;

        const responseB = await testsRequestCreator.createTest(requestBody, {});
        should(responseB.statusCode).eql(201);
        should(responseB.body).have.key('id');
        testIdB = responseB.body.id;
    });
    describe('Reports', function() {
        describe('Report Creation and Flow', function() {
            describe('Single Runner', function() {
                it('should fetch a report successfully with correct values and status initialize with no runners subscribed and under the time grace of initialization', async function () {
                    const jobName = 'jobName';
                    const id = uuid.v4();

                    nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                    const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                    expect(testCreateResponse.status).to.be.equal(201);

                    const testId = testCreateResponse.body.id;
                    const job = {
                        test_id: testId,
                        arrival_rate: 1,
                        duration: 1,
                        environment: 'test',
                        run_immediately: true,
                        type: 'load_test',
                        webhooks: [],
                        emails: []
                    };

                    const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                    expect(jobCreateResponse.status).to.be.equal(201);

                    const reportId = jobCreateResponse.body.report_id;
                    const jobId = jobCreateResponse.body.id;

                    const getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                    expect(getReportResponse.status).to.be.equal(200);
                    expect(getReportResponse.body).to.deep.contain({
                        status: 'initializing',
                        job_id: jobId,
                        report_id: reportId.toString(),
                        test_id: testId,
                        notes: '',
                        parallelism: 1,
                        phase: '0',
                        job_type: 'load_test',
                        arrival_rate: 1,
                        avg_rps: 0,
                        duration: 1,
                        environment: 'test',
                        is_favorite: false,
                        test_name: 'Create token and create customer',
                        test_type: 'basic'
                    });
                });
                it('should fetch a report successfully with status started after a runner subscribed and post stats of started phase', async function () {
                    const jobName = 'jobName';
                    const id = uuid.v4();

                    nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                    const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                    expect(testCreateResponse.status).to.be.equal(201);

                    const testId = testCreateResponse.body.id;
                    const job = {
                        test_id: testId,
                        arrival_rate: 1,
                        duration: 1,
                        environment: 'test',
                        run_immediately: true,
                        type: 'load_test',
                        webhooks: [],
                        emails: []
                    };

                    const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                    expect(jobCreateResponse.status).to.be.equal(201);

                    const reportId = jobCreateResponse.body.report_id;

                    await sleep(1 * 1000); // 1 seconds

                    const runnerId = uuid.v4();

                    const subscribeRunnerToReportResponse = await reportsRequestCreator.subscribeRunnerToReport(testId, reportId, runnerId);
                    expect(subscribeRunnerToReportResponse.status).to.be.equal(204);

                    const statsFromRunner = statsGenerator.generateStats(constants.SUBSCRIBER_STARTED_STAGE, runnerId);

                    const postStatsResponse = await reportsRequestCreator.postStats(testId, reportId, statsFromRunner);
                    expect(postStatsResponse.status).to.be.equal(204);

                    const getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                    expect(getReportResponse.status).to.be.equal(200);
                    expect(getReportResponse.body).to.have.property('status').and.to.be.equal(constants.REPORT_STARTED_STATUS);
                });
                it('should fetch a report successfully with status done after a runner subscribed and posted stats 2 times', async function() {
                    const jobName = 'jobName';
                    const id = uuid.v4();
                    const runnerId = uuid.v4();

                    nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                    const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                    expect(testCreateResponse.status).to.be.equal(201);

                    const testId = testCreateResponse.body.id;
                    const job = {
                        test_id: testId,
                        arrival_rate: 1,
                        duration: 1,
                        environment: 'test',
                        run_immediately: true,
                        type: 'load_test',
                        webhooks: [],
                        emails: []
                    };

                    const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                    expect(jobCreateResponse.status).to.be.equal(201);

                    const reportId = jobCreateResponse.body.report_id;

                    await assertRunnerSubscriptionToReport(testId, reportId, runnerId);
                    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_STARTED_STAGE);
                    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_INTERMEDIATE_STAGE);
                    await assertReportStatus(testId, reportId, constants.REPORT_IN_PROGRESS_STATUS);
                    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_INTERMEDIATE_STAGE);
                    await assertReportStatus(testId, reportId, constants.REPORT_IN_PROGRESS_STATUS);
                    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_DONE_STAGE);
                });
                it('should fetch a report successfully with status failed after a runner subscribed and aborted', async function() {
                    const jobName = 'jobName';
                    const id = uuid.v4();
                    const runnerId = uuid.v4();

                    nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                    const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                    expect(testCreateResponse.status).to.be.equal(201);

                    const testId = testCreateResponse.body.id;
                    const job = {
                        test_id: testId,
                        arrival_rate: 1,
                        duration: 1,
                        environment: 'test',
                        run_immediately: true,
                        type: 'load_test',
                        webhooks: [],
                        emails: []
                    };

                    const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                    expect(jobCreateResponse.status).to.be.equal(201);
                    const reportId = jobCreateResponse.body.report_id;

                    await assertRunnerSubscriptionToReport(testId, reportId, runnerId);
                    await assertPostStats(testId, reportId, runnerId, constants.REPORT_ABORTED_STATUS);
                    await assertReportStatus(testId, reportId, constants.REPORT_ABORTED_STATUS);
                });
                it('should create a report successfully - a complete happy flow cycle', async function () {
                    const jobName = 'jobName';
                    const id = uuid.v4();
                    const runnerId = uuid.v4();

                    nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                    const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                    expect(testCreateResponse.status).to.be.equal(201);

                    const testId = testCreateResponse.body.id;
                    const job = {
                        test_id: testId,
                        arrival_rate: 1,
                        duration: 1,
                        environment: 'test',
                        run_immediately: true,
                        type: 'load_test',
                        webhooks: [],
                        emails: []
                    };

                    const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                    expect(jobCreateResponse.status).to.be.equal(201);
                    const reportId = jobCreateResponse.body.report_id;

                    await sleep(1 * 1000); // 1 seconds
                    await runFullSingleRunnerCycle(testId, reportId, runnerId);
                });
                it('should fetch a report successfully with correct values and status initialize with no runners subscribed and under the time grace of initialization - functional_test', async function() {
                    const jobName = 'jobName';
                    const id = uuid.v4();

                    nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                    const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                    expect(testCreateResponse.status).to.be.equal(201);

                    const testId = testCreateResponse.body.id;
                    const job = {
                        test_id: testId,
                        arrival_count: 10,
                        duration: 1,
                        environment: 'test',
                        run_immediately: true,
                        type: 'functional_test',
                        webhooks: [],
                        emails: []
                    };

                    const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                    expect(jobCreateResponse.status).to.be.equal(201);

                    const reportId = jobCreateResponse.body.report_id;
                    const jobId = jobCreateResponse.body.id;

                    await sleep(1 * 1000);

                    const getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                    expect(getReportResponse.status).to.be.equal(200);
                    expect(getReportResponse.body).to.deep.contain({
                        status: 'initializing',
                        job_id: jobId,
                        report_id: reportId.toString(),
                        test_id: testId,
                        notes: '',
                        parallelism: 1,
                        phase: '0',
                        job_type: 'functional_test',
                        avg_rps: 0,
                        duration: 1,
                        environment: 'test',
                        is_favorite: false,
                        test_name: 'Create token and create customer',
                        test_type: 'basic'
                    });
                    expect(getReportResponse.body.arrival_rate).eql(undefined);
                });
                it('should fetch a report successfully with correct aggregated report assertions', async function () {
                    const jobName = 'jobName';
                    const id = uuid.v4();
                    const runnerId = uuid.v4();

                    nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                    const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                    expect(testCreateResponse.status).to.be.equal(201);

                    const testId = testCreateResponse.body.id;
                    const job = {
                        test_id: testId,
                        arrival_rate: 1,
                        duration: 1,
                        environment: 'test',
                        run_immediately: true,
                        type: 'load_test',
                        webhooks: [],
                        emails: []
                    };

                    const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                    expect(jobCreateResponse.status).to.be.equal(201);
                    const reportId = jobCreateResponse.body.report_id;

                    await assertRunnerSubscriptionToReport(testId, reportId, runnerId);

                    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_STARTED_STAGE);
                    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_INTERMEDIATE_STAGE);
                    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_INTERMEDIATE_STAGE);
                    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_DONE_STAGE);

                    const getAggregatedReportResponse = await reportsRequestCreator.getAggregatedReport(testId, reportId);
                    expect(getAggregatedReportResponse.body.aggregate.assertions).to.be.deep.equal({
                        '/users': {
                            'statusCode 201': {
                                success: 0,
                                fail: 594,
                                failureResponses: {
                                    200: 594
                                }
                            },
                            'header content-type values equals json': {
                                success: 100,
                                fail: 494,
                                failureResponses: {
                                    'application/json; charset=utf-8': 494
                                }
                            },
                            'hasHeader proxy-id': {
                                success: 0,
                                fail: 594,
                                failureResponses: {
                                    'response has no proxy-id header': 594
                                }
                            }
                        },
                        '/accounts': {
                            'statusCode 201': {
                                success: 80,
                                fail: 0,
                                failureResponses: {}
                            },
                            'hasHeader proxy-id': {
                                success: 0,
                                fail: 80,
                                failureResponses: {
                                    'response has no proxy-id header': 80
                                }
                            }
                        }
                    });
                });
                it('should fetch a report successfully with done status - with benchmark data config for test', async () => {
                    const jobName = 'jobName';
                    const id = uuid.v4();
                    const runnerId = uuid.v4();

                    nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                    const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                    expect(testCreateResponse.status).to.be.equal(201);

                    const testId = testCreateResponse.body.id;
                    const job = {
                        test_id: testId,
                        arrival_rate: 1,
                        duration: 1,
                        environment: 'test',
                        run_immediately: true,
                        type: 'load_test',
                        webhooks: [],
                        emails: []
                    };

                    const benchmarkRequest = {
                        rps: {
                            count: 100,
                            mean: 90.99
                        },
                        latency: { median: 357.2, p95: 1042 },
                        errors: { errorTest: 1 },
                        codes: { codeTest: 1 }
                    };
                    const config = {
                        benchmark_threshold: 55,
                        benchmark_weights: {
                            percentile_ninety_five: { percentage: 20 },
                            percentile_fifty: { percentage: 30 },
                            server_errors_ratio: { percentage: 20 },
                            client_errors_ratio: { percentage: 20 },
                            rps: { percentage: 10 }
                        }
                    };
                    const configRes = await configRequestCreator.updateConfig(config);
                    expect(configRes.status).to.be.equal(200);

                    const benchmarkRes = await testsRequestCreator.createBenchmark(testId, benchmarkRequest, {});
                    expect(benchmarkRes.status).to.be.equal(201);

                    const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                    expect(jobCreateResponse.status).to.be.equal(201);
                    // casting is not cool
                    const reportId = jobCreateResponse.body.report_id.toString();

                    await assertRunnerSubscriptionToReport(testId, reportId, runnerId);
                    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_STARTED_STAGE);
                    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_INTERMEDIATE_STAGE);
                    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_DONE_STAGE);

                    const getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                    const getLastReport = await reportsRequestCreator.getLastReports(25);
                    expect(getReportResponse.status).to.be.equal(200);
                    expect(getLastReport.status).to.be.equal(200);

                    const report = getReportResponse.body;
                    const lastReports = getLastReport.body.filter(report => report.report_id === reportId);
                    const lastReport = lastReports[0];
                    expect(lastReport).to.be.not.equal(undefined);
                    expect(lastReport.report_id).to.be.equal(reportId);

                    expect(report.score).to.be.equal(100);
                    expect(lastReport.score).to.be.equal(100);
                    expect(lastReport.benchmark_weights_data).to.be.deep.equal(report.benchmark_weights_data);
                    expect(report.benchmark_weights_data).to.be.deep.equal({
                        benchmark_threshold: 55,
                        rps: {
                            benchmark_value: 90.99,
                            report_value: 90.99,
                            percentage: 0.1,
                            score: 10
                        },
                        percentile_ninety_five: {
                            benchmark_value: 1042,
                            report_value: 1042,
                            percentage: 0.2,
                            score: 20
                        },
                        percentile_fifty: {
                            benchmark_value: 357.2,
                            report_value: 357.2,
                            percentage: 0.3,
                            score: 30
                        },
                        client_errors_ratio: {
                            benchmark_value: 0,
                            report_value: 0,
                            percentage: 0.2,
                            score: 20
                        },
                        server_errors_ratio: {
                            benchmark_value: 0.01,
                            report_value: 0,
                            percentage: 0.2,
                            score: 20
                        }
                    });

                    // cleanup
                    await assertConfigDeleteKey(config.BENCHMARK_WEIGHTS);
                    await assertConfigDeleteKey(config.BENCHMARK_THRESHOLD);
                });
                it('should fetch a report successfully with done status after runner subscribed and sent immediately a done subscriber status ', async function () {
                    const jobName = 'jobName';
                    const id = uuid.v4();
                    const runnerId = uuid.v4();

                    nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                    const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                    expect(testCreateResponse.status).to.be.equal(201);

                    const testId = testCreateResponse.body.id;
                    const job = {
                        test_id: testId,
                        arrival_rate: 1,
                        duration: 1,
                        environment: 'test',
                        run_immediately: true,
                        type: 'load_test',
                        webhooks: [],
                        emails: []
                    };

                    const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                    expect(jobCreateResponse.status).to.be.equal(201);
                    // casting is not cool
                    const reportId = jobCreateResponse.body.report_id.toString();
                    const jobId = jobCreateResponse.body.id;

                    await assertRunnerSubscriptionToReport(testId, reportId, runnerId);

                    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_DONE_STAGE);
                    const getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                    expect(getReportResponse.status).to.be.equal(200);
                    expect(getReportResponse.body).to.be.deep.contain({
                        test_id: testId,
                        test_name: basicTest.name,
                        report_id: reportId,
                        job_id: jobId,
                        job_type: job.type,
                        is_favorite: false,
                        test_type: basicTest.type,
                        phase: '0',
                        arrival_rate: 1,
                        duration: 1,
                        parallelism: 1,
                        notes: '',
                        environment: job.environment,
                        subscribers: [
                            {
                                runner_id: runnerId,
                                phase_status: constants.SUBSCRIBER_DONE_STAGE,
                                last_stats: null
                            }
                        ],
                        last_rps: 0,
                        avg_rps: 0,
                        last_success_rate: null,
                        status: 'finished'
                    });
                });
                it('should fetch a report successfully with failed status after a runner posted error stats', async function () {
                    const jobName = 'jobName';
                    const id = uuid.v4();
                    const runnerId = uuid.v4();

                    nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                    const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                    expect(testCreateResponse.status).to.be.equal(201);

                    const testId = testCreateResponse.body.id;
                    const job = {
                        test_id: testId,
                        arrival_rate: 1,
                        duration: 1,
                        environment: 'test',
                        run_immediately: true,
                        type: 'load_test',
                        webhooks: [],
                        emails: []
                    };

                    const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                    expect(jobCreateResponse.status).to.be.equal(201);
                    // casting is not cool
                    const reportId = jobCreateResponse.body.report_id.toString();

                    await assertRunnerSubscriptionToReport(testId, reportId, runnerId);
                    await sleep(1 * 1000);

                    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_STARTED_STAGE);
                    let getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                    expect(getReportResponse.status).to.be.equal(200);
                    expect(getReportResponse.body.status).to.be.equal(constants.REPORT_STARTED_STATUS);

                    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_INTERMEDIATE_STAGE);
                    getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                    expect(getReportResponse.status).to.be.equal(200);
                    expect(getReportResponse.body.status).to.be.equal(constants.REPORT_IN_PROGRESS_STATUS);

                    await assertPostStats(testId, reportId, runnerId, 'error');
                    getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                    expect(getReportResponse.status).to.be.equal(200);
                    expect(getReportResponse.body).to.have.property('status').and.to.be.equal(constants.REPORT_FAILED_STATUS);
                });
                it('should fetch a report successfully with aborted status and correct subscribers data after a runner subscribed -> sends stats -> sends aborted', async function () {
                    const jobName = 'jobName';
                    const id = uuid.v4();
                    const runnerId = uuid.v4();

                    nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                    const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                    expect(testCreateResponse.status).to.be.equal(201);

                    const testId = testCreateResponse.body.id;
                    const job = {
                        test_id: testId,
                        arrival_rate: 1,
                        duration: 1,
                        environment: 'test',
                        run_immediately: true,
                        type: 'load_test',
                        webhooks: [],
                        emails: []
                    };

                    const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                    expect(jobCreateResponse.status).to.be.equal(201);
                    // casting is not cool
                    const reportId = jobCreateResponse.body.report_id.toString();
                    const jobId = jobCreateResponse.body.id;

                    await assertRunnerSubscriptionToReport(testId, reportId, runnerId);

                    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_STARTED_STAGE);
                    let getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                    expect(getReportResponse.status).to.be.equal(200);
                    expect(getReportResponse.body).to.have.property('status').to.be.equal(constants.REPORT_STARTED_STATUS);

                    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_INTERMEDIATE_STAGE);
                    getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                    expect(getReportResponse.status).to.be.equal(200);
                    expect(getReportResponse.body).to.have.property('status').to.be.equal(constants.REPORT_IN_PROGRESS_STATUS);

                    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_ABORTED_STAGE);
                    getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                    expect(getReportResponse.status).to.be.equal(200);
                    expect(getReportResponse.body).to.have.property('status').to.be.equal(constants.REPORT_ABORTED_STATUS);
                    expect(getReportResponse.body).to.deep.contain({
                        test_id: testId,
                        test_name: basicTest.name,
                        report_id: reportId,
                        job_id: jobId,
                        job_type: job.type,
                        is_favorite: false,
                        test_type: basicTest.type,
                        phase: '0',
                        arrival_rate: 1,
                        duration: 1,
                        parallelism: 1,
                        notes: '',
                        environment: job.environment,
                        last_rps: 90.99,
                        last_success_rate: 100,
                        status: constants.REPORT_ABORTED_STATUS
                    });
                    expect(getReportResponse.body.subscribers).to.have.lengthOf(1);
                    expect(getReportResponse.body.subscribers[0]).to.be.deep.contains({
                        runner_id: runnerId,
                        phase_status: constants.SUBSCRIBER_ABORTED_STAGE
                    });
                    expect(getReportResponse.body.subscribers[0].last_stats).to.be.deep.contain({
                        scenariosCreated: 101,
                        scenariosCompleted: 101,
                        requestsCompleted: 101,
                        latency: {
                            min: 258.2,
                            max: 1060.6,
                            median: 357.2,
                            p95: 1042,
                            p99: 1059
                        },
                        rps: {
                            count: 101,
                            mean: 90.99,
                            total_count: 101
                        },
                        scenarioDuration: {
                            min: 259.5,
                            max: 1062.2,
                            median: 359.3,
                            p95: 1044.3,
                            p99: 1060.6
                        },
                        scenarioCounts: {
                            'Get response code 200': 101
                        },
                        errors: {},
                        codes: {
                            200: 101
                        },
                        assertions: {
                            '/users': {
                                'statusCode 201': {
                                    success: 0,
                                    fail: 297,
                                    failureResponses: {
                                        200: 297
                                    }
                                },
                                'header content-type values equals json': {
                                    success: 50,
                                    fail: 247,
                                    failureResponses: {
                                        'application/json; charset=utf-8': 247
                                    }
                                },
                                'hasHeader proxy-id': {
                                    success: 0,
                                    fail: 297,
                                    failureResponses: {
                                        'response has no proxy-id header': 297
                                    }
                                }
                            },
                            '/accounts': {
                                'statusCode 201': {
                                    success: 40,
                                    fail: 0,
                                    failureResponses: {}
                                },
                                'hasHeader proxy-id': {
                                    success: 0,
                                    fail: 40,
                                    failureResponses: {
                                        'response has no proxy-id header': 40
                                    }
                                }
                            }
                        },
                        matches: 0,
                        customStats: {},
                        counters: {},
                        concurrency: 0,
                        pendingRequests: 0,
                        scenariosAvoided: 0
                    });
                    const { start_time: startTime, end_time: endTime } = getReportResponse.body;
                    const startDate = new Date(startTime);
                    const endDate = new Date(endTime);
                    expect(getReportResponse.body.duration_seconds).to.be.equal((endDate.getTime() - startDate.getTime()) / 1000);
                });
                it('should send an email successfully after a job has finished and the report is done', async function() {
                    const jobName = 'jobName';
                    const id = uuid.v4();
                    const runnerId = uuid.v4();

                    nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                    const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                    expect(testCreateResponse.status).to.be.equal(201);

                    const testId = testCreateResponse.body.id;
                    const job = {
                        test_id: testId,
                        arrival_rate: 1,
                        duration: 1,
                        environment: 'test',
                        run_immediately: true,
                        type: 'load_test',
                        webhooks: [],
                        emails: ['wuff@predator.com']
                    };

                    const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                    expect(jobCreateResponse.status).to.be.equal(201);

                    const reportId = jobCreateResponse.body.report_id;
                    await mailhogHelper.clearAllOldMails();
                    await runFullSingleRunnerCycle(testId, reportId, runnerId);
                    await mailhogHelper.validateEmail();
                });
            });
            describe('Multiple Runners', function() {
                it('All runners post "done" stats - report finished', async function () {
                    const jobName = 'jobName';
                    const id = uuid.v4();
                    const firstRunnerId = uuid.v4();
                    const secondRunnerId = uuid.v4();

                    nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                    const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                    expect(testCreateResponse.status).to.be.equal(201);

                    const testId = testCreateResponse.body.id;
                    const job = {
                        test_id: testId,
                        arrival_rate: 1,
                        duration: 1,
                        environment: 'test',
                        run_immediately: true,
                        type: 'load_test',
                        webhooks: [],
                        emails: []
                    };

                    const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                    expect(jobCreateResponse.status).to.be.equal(201);
                    // casting is not cool
                    const reportId = jobCreateResponse.body.report_id.toString();
                    const jobId = jobCreateResponse.body.id;

                    await assertRunnerSubscriptionToReport(testId, reportId, firstRunnerId);
                    await assertRunnerSubscriptionToReport(testId, reportId, secondRunnerId);

                    await sleep(1 * 1000);

                    await assertPostStats(testId, reportId, firstRunnerId, constants.SUBSCRIBER_DONE_STAGE);
                    await assertPostStats(testId, reportId, secondRunnerId, constants.SUBSCRIBER_DONE_STAGE);

                    const getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                    expect(getReportResponse.status).to.be.equal(200);
                    expect(getReportResponse.body).to.be.deep.contain({
                        test_id: testId,
                        test_name: basicTest.name,
                        report_id: reportId,
                        job_id: jobId,
                        job_type: job.type,
                        is_favorite: false,
                        test_type: basicTest.type,
                        phase: '0',
                        arrival_rate: 1,
                        duration: 1,
                        parallelism: 1,
                        notes: '',
                        environment: job.environment,
                        last_rps: 0,
                        avg_rps: 0,
                        last_success_rate: null,
                        status: 'finished'
                    });
                    expect(getReportResponse.body.subscribers).to.be.an('array').and.to.have.lengthOf(2);
                    const firstSub = getReportResponse.body.subscribers.filter(sub => sub.runner_id === firstRunnerId)[0];
                    const secondSub = getReportResponse.body.subscribers.filter(sub => sub.runner_id === secondRunnerId)[0];
                    // eslint-disable-next-line no-unused-expressions
                    expect(firstSub).to.exist.and.to.be.deep.equal({
                        runner_id: firstRunnerId,
                        phase_status: constants.SUBSCRIBER_DONE_STAGE,
                        last_stats: null
                    });
                    expect(secondSub).to.exist.and.to.be.deep.equal({
                        runner_id: secondRunnerId,
                        phase_status: constants.SUBSCRIBER_DONE_STAGE,
                        last_stats: null
                    });
                });
                it('All runners post "error" stats - report failed', async function () {
                    const jobName = 'jobName';
                    const id = uuid.v4();
                    const firstRunnerId = uuid.v4();
                    const secondRunnerId = uuid.v4();

                    nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                    const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                    expect(testCreateResponse.status).to.be.equal(201);

                    const testId = testCreateResponse.body.id;
                    const job = {
                        test_id: testId,
                        arrival_rate: 1,
                        duration: 1,
                        environment: 'test',
                        run_immediately: true,
                        type: 'load_test',
                        webhooks: [],
                        emails: []
                    };

                    const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                    expect(jobCreateResponse.status).to.be.equal(201);
                    // casting is not cool
                    const reportId = jobCreateResponse.body.report_id.toString();

                    await assertRunnerSubscriptionToReport(testId, reportId, firstRunnerId);
                    await assertRunnerSubscriptionToReport(testId, reportId, secondRunnerId);

                    await assertPostStats(testId, reportId, firstRunnerId, 'error');
                    await assertPostStats(testId, reportId, secondRunnerId, 'error');

                    const getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                    expect(getReportResponse.status).to.be.equal(200);
                    expect(getReportResponse.body).to.have.property('status').and.to.be.equal('failed');
                });
                it('All runners post "aborted" stats - report aborted', async function () {
                    const jobName = 'jobName';
                    const id = uuid.v4();
                    const firstRunnerId = uuid.v4();
                    const secondRunnerId = uuid.v4();

                    nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                    const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                    expect(testCreateResponse.status).to.be.equal(201);

                    const testId = testCreateResponse.body.id;
                    const job = {
                        test_id: testId,
                        arrival_rate: 1,
                        duration: 1,
                        environment: 'test',
                        run_immediately: true,
                        type: 'load_test',
                        webhooks: [],
                        emails: []
                    };

                    const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                    expect(jobCreateResponse.status).to.be.equal(201);
                    // casting is not cool
                    const reportId = jobCreateResponse.body.report_id.toString();

                    await assertRunnerSubscriptionToReport(testId, reportId, firstRunnerId);
                    await assertRunnerSubscriptionToReport(testId, reportId, secondRunnerId);

                    await assertPostStats(testId, reportId, firstRunnerId, constants.SUBSCRIBER_ABORTED_STAGE);
                    await assertPostStats(testId, reportId, secondRunnerId, constants.REPORT_ABORTED_STATUS);
                    const getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                    expect(getReportResponse.status).to.be.equal(200);
                    expect(getReportResponse.body).to.have.property('status').to.be.equal(constants.REPORT_ABORTED_STATUS);
                });
<<<<<<< HEAD
                it('One runner post "error" stats - report partially finished', async function () {
                    const jobName = 'jobName';
                    const id = uuid.v4();
                    const firstRunnerId = uuid.v4();
                    const secondRunnerId = uuid.v4();

                    nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                    const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                    expect(testCreateResponse.status).to.be.equal(201);

                    const testId = testCreateResponse.body.id;
                    const job = {
                        test_id: testId,
                        arrival_rate: 1,
                        duration: 1,
                        environment: 'test',
                        run_immediately: true,
                        type: 'load_test',
                        webhooks: [],
                        emails: []
=======
            });
        });

        describe('Get report', function () {
            const getReportTestId = uuid();
            let reportId;
            const reportBody = {
                revision_id: uuid(),
                test_type: 'basic',
                test_name: 'integration-test',
                test_description: 'doing some integration testing',
                start_time: Date.now().toString(),
                last_updated_at: Date.now().toString()
            };
            describe('report of load_test', function () {
                before(async () => {
                    const jobOptions = {
                        arrival_rate: 10
>>>>>>> master
                    };

                    const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                    expect(jobCreateResponse.status).to.be.equal(201);
                    // casting is not cool
                    const reportId = jobCreateResponse.body.report_id.toString();

                    await assertRunnerSubscriptionToReport(testId, reportId, firstRunnerId);
                    await assertRunnerSubscriptionToReport(testId, reportId, secondRunnerId);

                    await assertPostStats(testId, reportId, firstRunnerId, constants.SUBSCRIBER_STARTED_STAGE);
                    await assertPostStats(testId, reportId, secondRunnerId, constants.SUBSCRIBER_STARTED_STAGE);
                    await assertPostStats(testId, reportId, firstRunnerId, constants.SUBSCRIBER_INTERMEDIATE_STAGE);
                    await assertReportStatus(testId, reportId, constants.REPORT_IN_PROGRESS_STATUS);
                    await assertPostStats(testId, reportId, secondRunnerId, 'error');
                    await assertReportStatus(testId, reportId, constants.REPORT_IN_PROGRESS_STATUS);
                    await assertPostStats(testId, reportId, firstRunnerId, constants.SUBSCRIBER_DONE_STAGE);
                    await assertReportStatus(testId, reportId, constants.REPORT_PARTIALLY_FINISHED_STATUS);
                });
                it('should successfully create report', async function () {
                    const jobName = 'jobName';
                    const id = uuid.v4();
                    const firstRunnerId = uuid.v4();
                    const secondRunnerId = uuid.v4();

                    nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                    const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                    expect(testCreateResponse.status).to.be.equal(201);

                    const testId = testCreateResponse.body.id;
                    const job = {
                        test_id: testId,
                        arrival_rate: 1,
                        duration: 1,
                        environment: 'test',
                        run_immediately: true,
                        type: 'load_test',
                        webhooks: [],
                        emails: []
                    };

                    const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                    expect(jobCreateResponse.status).to.be.equal(201);
                    // casting is not cool
                    const reportId = jobCreateResponse.body.report_id.toString();

                    await assertRunnerSubscriptionToReport(testId, reportId, firstRunnerId);
                    await assertRunnerSubscriptionToReport(testId, reportId, secondRunnerId);

                    await assertReportStatus(testId, reportId, constants.REPORT_INITIALIZING_STATUS);

                    await assertPostStats(testId, reportId, firstRunnerId, constants.SUBSCRIBER_STARTED_STAGE);
                    await assertPostStats(testId, reportId, secondRunnerId, constants.SUBSCRIBER_STARTED_STAGE);
                    await assertPostStats(testId, reportId, firstRunnerId, constants.SUBSCRIBER_INTERMEDIATE_STAGE);
                    await assertReportStatus(testId, reportId, constants.REPORT_IN_PROGRESS_STATUS);
                    await assertPostStats(testId, reportId, secondRunnerId, constants.SUBSCRIBER_INTERMEDIATE_STAGE);
                    await assertReportStatus(testId, reportId, constants.REPORT_IN_PROGRESS_STATUS);
                    await assertPostStats(testId, reportId, firstRunnerId, constants.SUBSCRIBER_DONE_STAGE);
                    await assertReportStatus(testId, reportId, constants.REPORT_PARTIALLY_FINISHED_STATUS);
                    await assertPostStats(testId, reportId, secondRunnerId, constants.SUBSCRIBER_DONE_STAGE);
                    await assertReportStatus(testId, reportId, constants.REPORT_FINISHED_STATUS);
                });
                it('One runner post "aborted" stats - report partially finished', async function () {
                    const jobName = 'jobName';
                    const id = uuid.v4();
                    const firstRunnerId = uuid.v4();
                    const secondRunnerId = uuid.v4();

                    nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                    const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                    expect(testCreateResponse.status).to.be.equal(201);

                    const testId = testCreateResponse.body.id;
                    const job = {
                        test_id: testId,
                        arrival_rate: 1,
                        duration: 1,
                        environment: 'test',
                        run_immediately: true,
                        type: 'load_test',
                        webhooks: [],
                        emails: []
                    };

                    const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                    expect(jobCreateResponse.status).to.be.equal(201);
                    // casting is not cool
                    const reportId = jobCreateResponse.body.report_id.toString();

                    await assertRunnerSubscriptionToReport(testId, reportId, firstRunnerId);
                    await assertRunnerSubscriptionToReport(testId, reportId, secondRunnerId);

                    await assertPostStats(testId, reportId, firstRunnerId, constants.SUBSCRIBER_STARTED_STAGE);
                    await assertPostStats(testId, reportId, secondRunnerId, constants.SUBSCRIBER_STARTED_STAGE);
                    await assertPostStats(testId, reportId, firstRunnerId, constants.SUBSCRIBER_INTERMEDIATE_STAGE);
                    await assertReportStatus(testId, reportId, constants.REPORT_IN_PROGRESS_STATUS);
                    await assertPostStats(testId, reportId, secondRunnerId, constants.SUBSCRIBER_ABORTED_STAGE);
                    await assertReportStatus(testId, reportId, constants.REPORT_IN_PROGRESS_STATUS);
                    await assertPostStats(testId, reportId, firstRunnerId, constants.SUBSCRIBER_DONE_STAGE);
                    await assertReportStatus(testId, reportId, constants.REPORT_PARTIALLY_FINISHED_STATUS);
                });
            });
        });
        describe('editReport', function() {
            it('Create a report -> edit notes', async function() {
                const jobName = 'jobName';
                const id = uuid.v4();

                nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                expect(testCreateResponse.status).to.be.equal(201);

                const testId = testCreateResponse.body.id;
                const job = {
                    test_id: testId,
                    arrival_rate: 1,
                    duration: 1,
                    environment: 'test',
                    run_immediately: true,
                    type: 'load_test',
                    notes: 'cats',
                    webhooks: [],
                    emails: []
                };
                const newNote = 'dogs';

                const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                expect(jobCreateResponse.status).to.be.equal(201);

                const reportId = jobCreateResponse.body.report_id;
                const deleteRunningTestResponse = await reportsRequestCreator.deleteReport(testId, reportId);
                deleteRunningTestResponse.statusCode.should.eql(409);
                deleteRunningTestResponse.body.should.eql({
                    message: "Can't delete running test with status initializing"
                });
            });
            });
            it('Create a report -> mark report as favorite', async function () {
                const jobName = 'jobName';
                const id = uuid.v4();

                nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                expect(testCreateResponse.status).to.be.equal(201);

                const testId = testCreateResponse.body.id;
                const job = {
                    test_id: testId,
                    arrival_rate: 1,
                    duration: 1,
                    environment: 'test',
                    run_immediately: true,
                    type: 'load_test',
                    notes: 'cats',
                    webhooks: [],
                    emails: []
                };

                const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                expect(jobCreateResponse.status).to.be.equal(201);

                const reportId = jobCreateResponse.body.report_id;

                const editReportResponse = await reportsRequestCreator.editReport(testId, reportId, { is_favorite: true });
                expect(editReportResponse.status).to.be.equal(204);

                const getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                expect(getReportResponse.status).to.be.equal(200);
                expect(getReportResponse.body).to.have.property('is_favorite').and.to.be.equal(true);
            });
            it('Create a report -> mark report as favorite and change the notes', async function () {
                const jobName = 'jobName';
                const id = uuid.v4();

                nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                expect(testCreateResponse.status).to.be.equal(201);

                const testId = testCreateResponse.body.id;
                const job = {
                    test_id: testId,
                    arrival_rate: 1,
                    duration: 1,
                    environment: 'test',
                    run_immediately: true,
                    type: 'load_test',
                    notes: 'cats',
                    webhooks: [],
                    emails: []
                };
                const newNote = 'dogs are my favorite';

                const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                expect(jobCreateResponse.status).to.be.equal(201);

                const reportId = jobCreateResponse.body.report_id;

                const editReportResponse = await reportsRequestCreator.editReport(testId, reportId, { is_favorite: true, notes: newNote });
                expect(editReportResponse.status).to.be.equal(204);

                const getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                expect(getReportResponse.status).to.be.equal(200);
                expect(getReportResponse.body).to.have.property('is_favorite').and.to.be.equal(true);
                expect(getReportResponse.body).to.have.property('notes').and.to.be.equal(newNote);
            });
        });
        describe('getReport', function() {
            it('Run a full cycle -> favorite report -> fetch report with is_favorite filter - should return the single created report', async function () {
                const jobName = 'jobName';
                const id = uuid.v4();
                const runnerId = uuid.v4();

                nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                expect(testCreateResponse.status).to.be.equal(201);

                const testId = testCreateResponse.body.id;
                const job = {
                    test_id: testId,
                    arrival_rate: 1,
                    duration: 1,
                    environment: 'test',
                    run_immediately: true,
                    type: 'load_test',
                    webhooks: [],
                    emails: []
                };

                const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                expect(jobCreateResponse.status).to.be.equal(201);
                const reportId = jobCreateResponse.body.report_id;

                await runFullSingleRunnerCycle(testId, reportId, runnerId);

                const editReportResponse = await reportsRequestCreator.editReport(testId, reportId, { is_favorite: true });
                expect(editReportResponse.status).to.be.equal(204);

                const createdReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                expect(createdReportResponse.status).to.be.equal(200);

                const getReportsResponse = await reportsRequestCreator.getReports(testId, 'is_favorite');

                expect(getReportsResponse.body).to.be.an('array').and.to.have.lengthOf(1);
                expect(getReportsResponse.body[0]).to.have.property('is_favorite').and.to.be.equal(true);
                expect(getReportsResponse.body[0]).to.be.deep.equal(createdReportResponse.body);
            });
            it('Run 2 full cycles -> favorite reports -> fetch reports with is_favorite filter - should return the 2 created report', async function () {
                const jobName = 'jobName';
                const id = uuid.v4();
                const firstRunnerId = uuid.v4();
                const secondRunnerId = uuid.v4();

                nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                expect(testCreateResponse.status).to.be.equal(201);

                const testId = testCreateResponse.body.id;
                const job = {
                    test_id: testId,
                    arrival_rate: 1,
                    duration: 1,
                    environment: 'test',
                    run_immediately: true,
                    type: 'load_test',
                    webhooks: [],
                    emails: []
                };

                const firstJobCreateResponse = await jobRequestCreator.createJob(job, headers);
                expect(firstJobCreateResponse.status).to.be.equal(201);
                const firstReportId = firstJobCreateResponse.body.report_id.toString();

                nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);
                const secondJobCreateResponse = await jobRequestCreator.createJob(job, headers);
                expect(secondJobCreateResponse.status).to.be.equal(201);
                const secondReportId = secondJobCreateResponse.body.report_id.toString();

                await runFullSingleRunnerCycle(testId, firstReportId, firstRunnerId);
                await runFullSingleRunnerCycle(testId, secondReportId, secondRunnerId);

                const editFirstReportResponse = await reportsRequestCreator.editReport(testId, firstReportId, { is_favorite: true });
                expect(editFirstReportResponse.status).to.be.equal(204);

                const editSecondReportResponse = await reportsRequestCreator.editReport(testId, secondReportId, { is_favorite: true });
                expect(editSecondReportResponse.status).to.be.equal(204);

                const firstCreatedReportResponse = await reportsRequestCreator.getReport(testId, firstReportId);
                expect(firstCreatedReportResponse.status).to.be.equal(200);

                const secondCreatedReportResponse = await reportsRequestCreator.getReport(testId, secondReportId);
                expect(secondCreatedReportResponse.status).to.be.equal(200);

                const getReportsResponse = await reportsRequestCreator.getReports(testId, 'is_favorite');

                expect(getReportsResponse.body).to.be.an('array').and.to.have.lengthOf(2);
                const firstReport = getReportsResponse.body.filter(report => report.report_id === firstReportId)[0];
                const secondReport = getReportsResponse.body.filter(report => report.report_id === secondReportId)[0];
                expect(firstReport).to.have.property('is_favorite').and.to.be.equal(true);
                expect(firstReport).to.be.deep.equal(firstCreatedReportResponse.body);
                expect(secondReport).to.have.property('is_favorite').and.to.be.equal(true);
                expect(secondReport).to.be.deep.equal(secondCreatedReportResponse.body);
            });
        });
        describe('deleteReport', async function() {
            it('Create a report -> delete it -> fetch it -> should return 404', async function() {
                const jobName = 'jobName';
                const id = uuid.v4();
                const runnerId = uuid.v4();

                nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                expect(testCreateResponse.status).to.be.equal(201);

                const testId = testCreateResponse.body.id;
                const job = {
                    test_id: testId,
                    arrival_rate: 1,
                    duration: 1,
                    environment: 'test',
                    run_immediately: true,
                    type: 'load_test',
                    webhooks: [],
                    emails: []
                };

                const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                expect(jobCreateResponse.status).to.be.equal(201);
                const reportId = jobCreateResponse.body.report_id;

                await runFullSingleRunnerCycle(testId, reportId, runnerId);

                const deleteResponse = await reportsRequestCreator.deleteReport(testId, reportId);
                expect(deleteResponse.status).to.be.equal(204);

                const getResponse = await reportsRequestCreator.getReport(testId, reportId);
                expect(getResponse.status).to.be.equal(404);
            });
            it('Delete report which is in progress', async function () {
                const jobName = 'jobName';
                const id = uuid.v4();

                nockK8sRunnerCreation(kubernetesConfig.kubernetesUrl, jobName, id, kubernetesConfig.kubernetesNamespace);

                const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                expect(testCreateResponse.status).to.be.equal(201);

                const testId = testCreateResponse.body.id;
                const job = {
                    test_id: testId,
                    arrival_rate: 1,
                    duration: 1,
                    environment: 'test',
                    run_immediately: true,
                    type: 'load_test',
                    webhooks: [],
                    emails: []
                };

                const jobCreateResponse = await jobRequestCreator.createJob(job, headers);
                expect(jobCreateResponse.status).to.be.equal(201);
                const reportId = jobCreateResponse.body.report_id;

                const deleteRunningTestResponse = await reportsRequestCreator.deleteReport(testId, reportId);
                expect(deleteRunningTestResponse.status).to.be.equal(409);
                expect(deleteRunningTestResponse.body).to.be.deep.equal({
                    message: "Can't delete running test with status initializing"
                });
            });
        });
        describe('container', function() {
            let testId,
                testIdA,
                testIdB,
                reportId,
                reportIdA,
                reportIdB,
                jobId,
                jobIdA,
                jobIdB,
                runnerId,
                runnerIdA,
                runnerIdB,
                firstRunner,
                secondRunner,
                jobBody,
                minimalReportBody,
                minimalReportBodyA,
                minimalReportBodyB;
            describe('Get exported reports', function () {
                before(async function () {
                    const jobResponse = await createJob(testId);
                    jobId = jobResponse.body.id;
                });
                beforeEach(async function () {
                    reportId = uuid();
                    runnerId = uuid();
                    const requestBody = require('../../testExamples/Basic_test');
                    const response = await testsRequestCreator.createTest(requestBody, {});
                    should(response.statusCode).be.eql(201);
                    should(response.body).have.key('id');
                    testId = response.body.id;

                    minimalReportBody = {
                        runner_id: runnerId,
                        test_type: 'basic',
                        report_id: reportId,
                        revision_id: uuid(),
                        job_id: jobId,
                        test_name: 'integration-test',
                        test_description: 'doing some integration testing',
                        start_time: Date.now().toString(),
                        last_updated_at: Date.now().toString(),
                        test_configuration: {
                            enviornment: 'test',
                            duration: 10,
                            arrival_rate: 20
                        }
                    };

                    const reportResponse = await reportsRequestCreator.createReport(testId, minimalReportBody);
                    should(reportResponse.statusCode).be.eql(201);
                });

                it('Post full cycle stats and export report', async function () {
                    const phaseStartedStatsResponse = await reportsRequestCreator.postStats(testId, reportId, statsGenerator.generateStats('started_phase', runnerId));
                    should(phaseStartedStatsResponse.statusCode).be.eql(204);

                    const getReport = await reportsRequestCreator.getReport(testId, reportId);
                    should(getReport.statusCode).be.eql(200);
                    const testStartTime = new Date(getReport.body.start_time);
                    const statDateFirst = new Date(testStartTime).setSeconds(testStartTime.getSeconds() + 20);
                    let intermediateStatsResponse = await reportsRequestCreator.postStats(testId, reportId, statsGenerator.generateStats('intermediate', runnerId, statDateFirst, 600));
                    should(intermediateStatsResponse.statusCode).be.eql(204);
                    let getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                    let report = getReportResponse.body;
                    should(report.avg_rps).eql(30);

                    const statDateSecond = new Date(testStartTime).setSeconds(testStartTime.getSeconds() + 40);
                    intermediateStatsResponse = await reportsRequestCreator.postStats(testId, reportId, statsGenerator.generateStats('intermediate', runnerId, statDateSecond, 200));
                    should(intermediateStatsResponse.statusCode).be.eql(204);
                    getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                    report = getReportResponse.body;
                    should(report.avg_rps).eql(20);

                    const statDateThird = new Date(testStartTime).setSeconds(testStartTime.getSeconds() + 60);
                    const doneStatsResponse = await reportsRequestCreator.postStats(testId, reportId, statsGenerator.generateStats('done', runnerId, statDateThird));
                    should(doneStatsResponse.statusCode).be.eql(204);
                    getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
                    should(getReportResponse.statusCode).be.eql(200);
                    report = getReportResponse.body;
                    should(report.avg_rps).eql(13.33);

                    const getExportedReportResponse = await reportsRequestCreator.getExportedReport(testId, reportId, 'csv');
                    should(getExportedReportResponse.statusCode).be.eql(200);

                    const contentDisposition = new RegExp('attachment; filename=integration-test_[a-f0-9\-]+_.*\.csv');
                    const contentType = new RegExp('text/csv; charset=utf-8');
                    const receivedDisposition = getExportedReportResponse.header['content-disposition'];
                    const receivedType = getExportedReportResponse.header['content-type'];

                    should(contentDisposition.test(receivedDisposition)).be.eql(true);
                    should(contentType.test(receivedType)).be.eql(true);
                    const EXPORTED_REPORT_HEADER = 'timestamp,timemillis,median,p95,p99,rps,status_200';
                    const reportLines = getExportedReportResponse.text.split('\n');
                    const headers = reportLines[0];
                    should(headers).eql(EXPORTED_REPORT_HEADER);
                });

                describe('export report with no stats', function () {
                    it('should return error 404', async function () {
                        const getReportResponse = await reportsRequestCreator.getExportedReport(testId, reportId, 'csv');
                        should(getReportResponse.statusCode).be.eql(404);
                    });
                });

                describe('export report of non existent report', function () {
                    it('should return error 404', async function () {
                        const getReportResponse = await reportsRequestCreator.getExportedReport(testId, 'null', 'csv');
                        should(getReportResponse.statusCode).be.eql(404);
                    });
                });
            });
            describe('Get exported comparison report', function () {

                before(async function () {
                    const jobResponseA = await createJob(testIdA);
                    jobIdA = jobResponseA.body.id;

                    const jobResponseB = await createJob(testIdB);
                    jobIdB = jobResponseB.body.id;
                });
                beforeEach(async function () {
                    reportIdA = uuid();
                    runnerIdA = uuid();
                    const requestBodyA = basicTest;
                    const responseA = await testsRequestCreator.createTest(requestBodyA, {});
                    should(responseA.statusCode).be.eql(201);
                    should(responseA.body).have.key('id');
                    testIdA = responseA.body.id;

                    minimalReportBodyA = {
                        runner_id: runnerIdA,
                        test_type: 'basic',
                        report_id: reportIdA,
                        revision_id: uuid(),
                        job_id: jobIdA,
                        test_name: 'integration-test',
                        test_description: 'doing some integration testing',
                        start_time: Date.now().toString(),
                        last_updated_at: Date.now().toString(),
                        test_configuration: {
                            enviornment: 'test',
                            duration: 10,
                            arrival_rate: 200
                        }
                    };

                    const reportResponseA = await reportsRequestCreator.createReport(testIdA, minimalReportBodyA);
                    should(reportResponseA.statusCode).be.eql(201);

                    reportIdB = uuid();
                    runnerIdB = uuid();
                    const requestBodyB = basicTest;
                    const responseB = await testsRequestCreator.createTest(requestBodyB, {});
                    should(responseB.statusCode).be.eql(201);
                    should(responseB.body).have.key('id');
                    testIdB = responseB.body.id;

                    minimalReportBodyB = {
                        runner_id: runnerIdB,
                        test_type: 'basic',
                        report_id: reportIdB,
                        revision_id: uuid(),
                        job_id: jobIdB,
                        test_name: 'integration-test',
                        test_description: 'doing some integration testing',
                        start_time: Date.now().toString(),
                        last_updated_at: Date.now().toString(),
                        test_configuration: {
                            enviornment: 'test',
                            duration: 10,
                            arrival_rate: 200
                        }
                    };

                    const reportResponseB = await reportsRequestCreator.createReport(testIdB, minimalReportBodyB);
                    should(reportResponseB.statusCode).be.eql(201);
                });

                it('Post full cycle stats for both reports and export report', async function () {
                    const phaseStartedStatsResponseA = await reportsRequestCreator.postStats(testIdA, reportIdA, statsGenerator.generateStats('started_phase', runnerIdA));
                    should(phaseStartedStatsResponseA.statusCode).be.eql(204);

                    const getReportA = await reportsRequestCreator.getReport(testIdA, reportIdA);
                    should(getReportA.statusCode).be.eql(200);
                    const testStartTimeA = new Date(getReportA.body.start_time);
                    const statDateFirstA = new Date(testStartTimeA).setSeconds(testStartTimeA.getSeconds() + 20);
                    let intermediateStatsResponseA = await reportsRequestCreator.postStats(testIdA, reportIdA, statsGenerator.generateStats('intermediate', runnerIdA, statDateFirstA, 600));
                    should(intermediateStatsResponseA.statusCode).be.eql(204);

                    const statDateSecondA = new Date(testStartTimeA).setSeconds(testStartTimeA.getSeconds() + 40);
                    intermediateStatsResponseA = await reportsRequestCreator.postStats(testIdA, reportIdA, statsGenerator.generateStats('intermediate', runnerIdA, statDateSecondA, 200));
                    should(intermediateStatsResponseA.statusCode).be.eql(204);

                    const statDateThirdA = new Date(testStartTimeA).setSeconds(testStartTimeA.getSeconds() + 60);
                    const doneStatsResponseA = await reportsRequestCreator.postStats(testIdA, reportIdA, statsGenerator.generateStats('done', runnerIdA, statDateThirdA));
                    should(doneStatsResponseA.statusCode).be.eql(204);

                    const phaseStartedStatsResponseB = await reportsRequestCreator.postStats(testIdB, reportIdB, statsGenerator.generateStats('started_phase', runnerIdB));
                    should(phaseStartedStatsResponseB.statusCode).be.eql(204);

                    const getReportB = await reportsRequestCreator.getReport(testIdB, reportIdB);
                    should(getReportB.statusCode).be.eql(200);
                    const testStartTimeB = new Date(getReportB.body.start_time);
                    const statDateFirstB = new Date(testStartTimeB).setSeconds(testStartTimeB.getSeconds() + 20);
                    let intermediateStatsResponseB = await reportsRequestCreator.postStats(testIdB, reportIdB, statsGenerator.generateStats('intermediate', runnerIdB, statDateFirstB, 600));
                    should(intermediateStatsResponseB.statusCode).be.eql(204);

                    const statDateSecondB = new Date(testStartTimeB).setSeconds(testStartTimeB.getSeconds() + 40);
                    intermediateStatsResponseB = await reportsRequestCreator.postStats(testIdB, reportIdB, statsGenerator.generateStats('intermediate', runnerIdB, statDateSecondB, 200));
                    should(intermediateStatsResponseB.statusCode).be.eql(204);

                    const statDateThirdB = new Date(testStartTimeB).setSeconds(testStartTimeB.getSeconds() + 60);
                    const doneStatsResponseB = await reportsRequestCreator.postStats(testIdB, reportIdB, statsGenerator.generateStats('done', runnerIdB, statDateThirdB));
                    should(doneStatsResponseB.statusCode).be.eql(204);

                    const reportMetaData = {
                        test_ids: [testIdA, testIdB],
                        report_ids: [reportIdA, reportIdB]
                    };

                    const getExportedCompareResponse = await reportsRequestCreator.getExportedCompareReport('csv', reportMetaData);
                    should(getExportedCompareResponse.statusCode).be.eql(200);
                    const contentDisposition = new RegExp('attachment; filename=integration-test_integration-test_comparison_[a-f0-9\-]+_[a-f0-9\-]+\.csv');
                    const contentType = new RegExp('text/csv; charset=utf-8');
                    const receivedDisposition = getExportedCompareResponse.header['content-disposition'];
                    const receivedType = getExportedCompareResponse.header['content-type'];

                    should(contentDisposition.test(receivedDisposition)).be.eql(true);
                    should(contentType.test(receivedType)).be.eql(true);
                    const EXPORTED_REPORT_HEADER = 'timestamp,timemillis,A_median,A_p95,A_p99,A_rps,A_status_200,B_median,B_p95,B_p99,B_rps,B_status_200';
                    const reportLines = getExportedCompareResponse.text.split('\n');
                    const headers = reportLines[0];
                    should(headers).eql(EXPORTED_REPORT_HEADER);
                });

                it('Post full cycle stats for both reports and give an unknown report', async function () {
                    const phaseStartedStatsResponseA = await reportsRequestCreator.postStats(testIdA, reportIdA, statsGenerator.generateStats('started_phase', runnerIdA));
                    should(phaseStartedStatsResponseA.statusCode).be.eql(204);

                    const getReportA = await reportsRequestCreator.getReport(testIdA, reportIdA);
                    should(getReportA.statusCode).be.eql(200);
                    const testStartTimeA = new Date(getReportA.body.start_time);
                    const statDateFirstA = new Date(testStartTimeA).setSeconds(testStartTimeA.getSeconds() + 20);
                    let intermediateStatsResponseA = await reportsRequestCreator.postStats(testIdA, reportIdA, statsGenerator.generateStats('intermediate', runnerIdA, statDateFirstA, 600));
                    should(intermediateStatsResponseA.statusCode).be.eql(204);

                    const statDateSecondA = new Date(testStartTimeA).setSeconds(testStartTimeA.getSeconds() + 40);
                    intermediateStatsResponseA = await reportsRequestCreator.postStats(testIdA, reportIdA, statsGenerator.generateStats('intermediate', runnerIdA, statDateSecondA, 200));
                    should(intermediateStatsResponseA.statusCode).be.eql(204);

                    const statDateThirdA = new Date(testStartTimeA).setSeconds(testStartTimeA.getSeconds() + 60);
                    const doneStatsResponseA = await reportsRequestCreator.postStats(testIdA, reportIdA, statsGenerator.generateStats('done', runnerIdA, statDateThirdA));
                    should(doneStatsResponseA.statusCode).be.eql(204);

                    const phaseStartedStatsResponseB = await reportsRequestCreator.postStats(testIdB, reportIdB, statsGenerator.generateStats('started_phase', runnerIdB));
                    should(phaseStartedStatsResponseB.statusCode).be.eql(204);

                    const getReportB = await reportsRequestCreator.getReport(testIdB, reportIdB);
                    should(getReportB.statusCode).be.eql(200);
                    const testStartTimeB = new Date(getReportB.body.start_time);
                    const statDateFirstB = new Date(testStartTimeB).setSeconds(testStartTimeB.getSeconds() + 20);
                    let intermediateStatsResponseB = await reportsRequestCreator.postStats(testIdB, reportIdB, statsGenerator.generateStats('intermediate', runnerIdB, statDateFirstB, 600));
                    should(intermediateStatsResponseB.statusCode).be.eql(204);

                    const statDateSecondB = new Date(testStartTimeB).setSeconds(testStartTimeB.getSeconds() + 40);
                    intermediateStatsResponseB = await reportsRequestCreator.postStats(testIdB, reportIdB, statsGenerator.generateStats('intermediate', runnerIdB, statDateSecondB, 200));
                    should(intermediateStatsResponseB.statusCode).be.eql(204);

                    const statDateThirdB = new Date(testStartTimeB).setSeconds(testStartTimeB.getSeconds() + 60);
                    const doneStatsResponseB = await reportsRequestCreator.postStats(testIdB, reportIdB, statsGenerator.generateStats('done', runnerIdB, statDateThirdB));
                    should(doneStatsResponseB.statusCode).be.eql(204);

                    const reportMetaData = {
                        test_ids: [testIdA, testIdB],
                        report_ids: ['unknown', reportIdB]
                    };

                    const getExportedCompareResponse = await reportsRequestCreator.getExportedCompareReport('csv', reportMetaData);
                    should(getExportedCompareResponse.statusCode).be.eql(404);
                });
            });
        });

    });
    describe('Sad flow', function() {
        describe('editReport', function() {
            it('when report id does not exist - should return 404', async function () {
                const reportId = Date.now();
                const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                expect(testCreateResponse.status).to.be.equal(201);

                const testId = testCreateResponse.body.id;

                const editReportResponse = await reportsRequestCreator.editReport(testId, reportId, { notes: 'some text' });
                expect(editReportResponse.status).eql(404);
                expect(editReportResponse.body).to.be.deep.equal({
                    message: 'Report not found'
                });
            });
        });
        describe('getReport', function() {
            it('GET not existing report', async function () {
                const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                expect(testCreateResponse.status).to.be.equal(201);

                const testId = testCreateResponse.body.id;

                const getReportResponse = await reportsRequestCreator.getReport(testId, uuid.v4());
                expect(getReportResponse.status).to.be.equal(404);
                expect(getReportResponse.body).to.be.deep.equal({
                    message: 'Report not found'
                });
            });
        });

        describe('postStats', function() {
            it('POST stats on not existing report', async function () {
                const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                expect(testCreateResponse.status).to.be.equal(201);

                const testId = testCreateResponse.body.id;

                const postStatsresponse = await reportsRequestCreator.postStats(testId, uuid(), statsGenerator.generateStats('started_phase', uuid()));
                expect(postStatsresponse.status).to.be.equal(404);
                expect(postStatsresponse.body).to.be.deep.equal({
                    message: 'Report not found'
                });
            });
        });
    });
    describe('Bad flow', function() {
        describe('editReport', function() {
            it('when edit additional properties that not include in the swagger', async function () {
                const reportId = Date.now();
                const testCreateResponse = await testsRequestCreator.createTest(basicTest, headers);
                expect(testCreateResponse.status).to.be.equal(201);

                const testId = testCreateResponse.body.id;

                const editReportResponse = await reportsRequestCreator.editReport(testId, reportId, { additional: 'add' });
                expect(editReportResponse.status).to.be.equal(400);
                expect(editReportResponse.body).to.be.deep.equal({
                    message: 'Input validation error',
                    validation_errors: [
                        "body should NOT have additional properties 'additional'"
                    ]
                });
            });
        });
        describe('lastReports', function() {
            it('GET last reports without limit query param', async function () {
                const lastReportsResponse = await reportsRequestCreator.getLastReports();
                expect(lastReportsResponse.status).to.be.equal(400);
                expect(lastReportsResponse.body).to.be.deep.equal({
                    message: 'Input validation error',
                    validation_errors: [
                        'query/limit should be integer'
                    ]
                });
            });

            it('GET last reports without limit query param', async function () {
                const lastReportsResponse = await reportsRequestCreator.getLastReports();
                expect(lastReportsResponse.status).to.be.equal(400);
                expect(lastReportsResponse.body).to.be.deep.equal({
                    message: 'Input validation error',
                    validation_errors: [
                        'query/limit should be integer'
                    ]
                });
            });

            it('GET last reports without limit query param', async function () {
                const lastReportsResponse = await reportsRequestCreator.getLastReports();
                expect(lastReportsResponse.status).to.be.equal(400);
                expect(lastReportsResponse.body).to.be.deep.equal({
                    message: 'Input validation error',
                    validation_errors: [
                        'query/limit should be integer'
                    ]
                });
            });
        });
        describe('postStats', function () {
            it('POST stats with bad request body', async function () {
                const postStatsResponse = await reportsRequestCreator.postStats(uuid(), uuid(), {});
                expect(postStatsResponse.status).to.be.equal(400);
                expect(postStatsResponse.body).to.be.deep.equal({
                    message: 'Input validation error',
                    validation_errors: [
                        'body should have required property \'stats_time\'',
                        'body should have required property \'phase_status\'',
                        'body should have required property \'data\''
                    ]
                });
            });

            it('POST stats with bad request body', async function () {
                const postStatsResponse = await reportsRequestCreator.postStats(uuid(), uuid(), {});
                expect(postStatsResponse.status).to.be.equal(400);
                expect(postStatsResponse.body).to.be.deep.equal({
                    message: 'Input validation error',
                    validation_errors: [
                        'body should have required property \'stats_time\'',
                        'body should have required property \'phase_status\'',
                        'body should have required property \'data\''
                    ]
                });
            });

            it('POST stats with bad request body', async function () {
                const postStatsResponse = await reportsRequestCreator.postStats(uuid(), uuid(), {});
                expect(postStatsResponse.status).to.be.equal(400);
                expect(postStatsResponse.body).to.be.deep.equal({
                    message: 'Input validation error',
                    validation_errors: [
                        'body should have required property \'stats_time\'',
                        'body should have required property \'phase_status\'',
                        'body should have required property \'data\''
                    ]
                });
            });
        });
    });
});

function nockK8sRunnerCreation(url, name, uid, namespace) {
    nock(url).persist()
        .post(`/apis/batch/v1/namespaces/${namespace}/jobs`)
        .reply(200, {
            metadata: { name, uid },
            namespace: namespace
        });
}

async function sleep(timeInMs) {
    return new Promise((resolve) => {
        setTimeout(resolve, timeInMs);
    });
}

async function assertReportStatus(testId, reportId, status) {
    const getReportResponse = await reportsRequestCreator.getReport(testId, reportId);
    expect(getReportResponse.status).to.be.equal(200);
    expect(getReportResponse.body).to.have.property('status').and.to.be.equal(status);
    return getReportResponse;
}

async function assertPostStats(testId, reportId, runnerId, status) {
    const statsFromRunnerDone = statsGenerator.generateStats(status, runnerId);
    const postStatsRunnerDone = await reportsRequestCreator.postStats(testId, reportId, statsFromRunnerDone);
    expect(postStatsRunnerDone.status).to.be.equal(204);
    return postStatsRunnerDone;
}

async function assertRunnerSubscriptionToReport(testId, reportId, runnerId) {
    const subscribeRunnerToReportResponse = await reportsRequestCreator.subscribeRunnerToReport(testId, reportId, runnerId);
    expect(subscribeRunnerToReportResponse.status).to.be.equal(204);
    return subscribeRunnerToReportResponse;
}

async function runFullSingleRunnerCycle(testId, reportId, runnerId) {
    await assertRunnerSubscriptionToReport(testId, reportId, runnerId);
    await assertReportStatus(testId, reportId, constants.REPORT_INITIALIZING_STATUS);
    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_STARTED_STAGE);
    await assertReportStatus(testId, reportId, constants.REPORT_STARTED_STATUS);
    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_INTERMEDIATE_STAGE);
    await assertReportStatus(testId, reportId, constants.REPORT_IN_PROGRESS_STATUS);
    await assertPostStats(testId, reportId, runnerId, constants.SUBSCRIBER_DONE_STAGE);
    await assertReportStatus(testId, reportId, constants.REPORT_FINISHED_STATUS);
}

async function assertConfigDeleteKey(key) {
    const configDeleteResponse = await configRequestCreator.deleteConfig(key);
    expect(configDeleteResponse.status).to.be.equal(204);
}
