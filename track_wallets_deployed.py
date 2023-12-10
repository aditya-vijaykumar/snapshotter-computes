import json
from typing import List
from typing import Tuple
from typing import Union

from redis import asyncio as aioredis

from .utils.event_log_decoder import EventLogDecoder
from .utils.models.registration_models import TrackingWalletsDeploymentSnapshot
from snapshotter.utils.callback_helpers import GenericProcessorSnapshot
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EthTransactionReceipt
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.redis.redis_keys import epoch_txs_htable
from snapshotter.utils.rpc import RpcHelper


class TrackingWalletDeploymentsProcessor(GenericProcessorSnapshot):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='TrackingWalletDeploymentsProcessor')

    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,

    ) -> Union[None, List[Tuple[str, TrackingWalletsDeploymentSnapshot]]]:
        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        if max_chain_height != min_chain_height:
            self._logger.error('Currently only supports single block height')
            raise Exception('Currently only supports single block height')

        # get txs for this epoch
        txs_hset = await redis_conn.hgetall(epoch_txs_htable(epoch.epochId))
        all_txs = {k.decode(): EthTransactionReceipt.parse_raw(v) for k, v in txs_hset.items()}

        contract_address = '0xEF211C252722D0d2C48482D4A5D1474792455eFF'
        contract_txs = list(
            map(
                lambda x: x.dict(), filter(
                    lambda tx: tx.to == contract_address,
                    all_txs.values(),
                ),
            ),
        )

        with open('snapshotter/modules/computes/static/abi/masala_master.json') as f:
            abi = json.load(f)

        node = rpc_helper.get_current_node()
        w3 = node['web3_client']
        contract = w3.eth.contract(address=contract_address, abi=abi)

        eld = EventLogDecoder(contract)

        snapshots = []
        processed_logs = []

        for tx in contract_txs:
            for log in tx['logs']:
                try:
                    processed_logs.append(eld.decode_log(log))
                except:
                    pass
        if processed_logs:
            for log in processed_logs:
                snapshots.append(
                        (
                            # log['username'].lower(),
                            TrackingWalletsDeploymentSnapshot(
                                wallet_address=log['newWallet'],
                                username=log['username'],
                            ),
                        ),
                    )

        return snapshots