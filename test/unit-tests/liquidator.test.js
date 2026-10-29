const sinon = require("sinon");
const { expect } = require("chai");
const Liquidator = require("../../src/web3client/liquidator");

const superToken = "0x46fd5cfB4c12D87acD3a13e92BAa53240C661D93";
const receiver = "0xd21549892BF317CCFe7fB220dcF14aB15dFE5428";
const criticalSender = "0x3496A0Da043108Ec81022ebEcced3D0cf821A58D";
const healthySender = "0x1111111111111111111111111111111111111111";

describe("Liquidator Tests", () => {
    let liquidator;
    let mockApp;

    // two due streams of one token: getNumberOfBatchCalls reports a batch, but only one stream is closable
    const flows = (pppmode) => [
        { superToken, sender: criticalSender, receiver, pppmode, source: "CFA", flowRate: 3805175038 },
        { superToken, sender: healthySender, receiver, pppmode, source: "CFA", flowRate: 380517504 }
    ];

    beforeEach(() => {
        mockApp = {
            _isShutdown: false,
            isRPCDrifting: () => false,
            config: {
                BATCH_CONTRACT: "0x6b008BAc0e5846cB5d9Ca02ca0e801fCbF88B6f9",
                TOKENS: [superToken],
                MAX_TX_NUMBER: 100,
                MAX_BATCH_TX: 10,
                ADDITIONAL_LIQUIDATION_DELAY: 0,
                RETRY_GAS_MULTIPLIER: 1.15,
                TX_TIMEOUT: 60000
            },
            time: { getTimeWithDelay: () => Date.now() },
            db: {
                bizQueries: {
                    getNumberOfBatchCalls: sinon.stub().resolves([{ superToken, numberTxs: 2 }]),
                    getLiquidations: sinon.stub()
                }
            },
            protocol: {
                PPPMode: { Patrician: 0, Pleb: 1, Pirate: 2 },
                checkFlow: sinon.stub().resolves({ flowRate: "1" }),
                isAccountCriticalNow: sinon.stub().callsFake(async (token, account) => account === criticalSender),
                // critical but still solvent: before the end of the liquidation period, so a pirate cannot close it yet
                isAccountSolventNow: sinon.stub().resolves(true),
                isPatricianPeriodNow: sinon.stub().resolves({ isPatricianPeriod: false }),
                generateDeleteCFAStreamTxData: sinon.stub().returns({ tx: "0xcfa", target: "0xbatch" }),
                generateDeleteGDAStreamTxData: sinon.stub().returns({ tx: "0xgda", target: "0xhost" })
            },
            queues: { addQueuedEstimation: sinon.stub().resolves() },
            timer: { timeout: sinon.stub().resolves() },
            client: {
                getAccount: () => ({ address: "0xsentinel", txCount: sinon.stub().resolves(7) }),
                getChainId: sinon.stub().resolves(8453)
            },
            gasEstimator: { getCappedGasPrice: sinon.stub().resolves({ gasPrice: 1000, hitGasPriceLimit: false }) },
            notifier: { sendNotification: sinon.stub() },
            logger: { info: sinon.stub(), debug: sinon.stub(), warn: sinon.stub(), error: sinon.stub() }
        };
        liquidator = new Liquidator(mockApp);
        liquidator.sendWithRetry = sinon.stub().resolves({ error: undefined, tx: {} });
        liquidator.sendBatch = sinon.stub().resolves();
    });

    afterEach(() => {
        sinon.restore();
    });

    it("#1.1 - patrician: a batch with a single closable stream should be sent as a CFA termination", async () => {
        mockApp.db.bizQueries.getLiquidations.resolves(flows(0));
        await liquidator.start();
        expect(liquidator.sendBatch.called).to.be.false;
        expect(liquidator.sendWithRetry.calledOnce).to.be.true;
        expect(mockApp.protocol.generateDeleteCFAStreamTxData.calledOnceWith(superToken, criticalSender, receiver)).to.be.true;
        expect(mockApp.protocol.generateDeleteGDAStreamTxData.called).to.be.false;
        // a patrician closes when critical, solvency is not a condition
        expect(mockApp.protocol.isAccountSolventNow.called).to.be.false;
    });

    it("#1.2 - pleb: a batch with a single closable stream should be sent once the patrician period is over", async () => {
        mockApp.db.bizQueries.getLiquidations.resolves(flows(1));
        await liquidator.start();
        expect(liquidator.sendWithRetry.calledOnce).to.be.true;
        expect(mockApp.protocol.generateDeleteCFAStreamTxData.calledOnceWith(superToken, criticalSender, receiver)).to.be.true;
        expect(mockApp.protocol.isAccountSolventNow.called).to.be.false;
    });

    it("#1.3 - pirate: a solvent account should not be sent", async () => {
        mockApp.db.bizQueries.getLiquidations.resolves(flows(2));
        await liquidator.start();
        expect(liquidator.sendWithRetry.called).to.be.false;
        expect(liquidator.sendBatch.called).to.be.false;
    });
});
