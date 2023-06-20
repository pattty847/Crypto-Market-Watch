from typing import Optional, List
from pydantic import BaseModel

class Fee(BaseModel):
    rate: Optional[float]
    cost: Optional[float]
    currency: str
    type: Optional[str]

class Info(BaseModel):
    type: str
    trade_id: int
    maker_order_id: str
    taker_order_id: str
    side: str
    size: float
    price: float
    product_id: str
    sequence: int
    time: str

class Trade(BaseModel):
    id: str
    order: Optional[str]
    info: Info
    timestamp: int
    datetime: str
    symbol: str
    type: Optional[str]
    takerOrMaker: str
    side: str
    price: float
    amount: float
    fee: Fee
    cost: float
    fees: List[Fee]