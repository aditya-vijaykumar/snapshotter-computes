from pydantic import BaseModel

class TrackingWalletsDeploymentSnapshot(BaseModel):
    wallet_address: str
    username: str