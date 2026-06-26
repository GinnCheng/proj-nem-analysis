# proj-nem-analysis





Help on OEClient in module openelectricity.client object:



class OEClient(BaseOEClient)

&#x20;|  OEClient(api\_key: str | None = None, base\_url: str | None = None, \*, proxy: str | None = None, proxy\_auth: aiohttp.helpers.BasicAuth | None = None, verify\_ssl: bool = True, ssl\_context: ssl.SSLContext | None = None, ca\_cert: str | os.PathLike\[str] | None = None, trust\_env: bool = False) -> None

&#x20;|  

&#x20;|  Synchronous client for the OpenElectricity API.

&#x20;|  

&#x20;|  It runs aiohttp under the hood and is safe to call from inside an existing

&#x20;|  event loop (e.g. a Jupyter/IPython notebook) — when a loop is already

&#x20;|  running, requests are dispatched to a worker thread.

&#x20;|  

&#x20;|  Method resolution order:

&#x20;|      OEClient

&#x20;|      BaseOEClient

&#x20;|      builtins.object

&#x20;|  

&#x20;|  Methods defined here:

&#x20;|  

&#x20;|  \_\_enter\_\_(self) -> 'OEClient'

&#x20;|  

&#x20;|  \_\_exit\_\_(self, exc\_type, exc\_val, exc\_tb) -> None

&#x20;|  

&#x20;|  \_\_init\_\_(self, api\_key: str | None = None, base\_url: str | None = None, \*, proxy: str | None = None, proxy\_auth: aiohttp.helpers.BasicAuth | None = None, verify\_ssl: bool = True, ssl\_context: ssl.SSLContext | None = None, ca\_cert: str | os.PathLike\[str] | None = None, trust\_env: bool = False) -> None

&#x20;|      Initialize self.  See help(type(self)) for accurate signature.

&#x20;|  

&#x20;|  close(self) -> None

&#x20;|      Close the underlying HTTP client.

&#x20;|  

&#x20;|  get\_current\_user(self) -> openelectricity.models.user.OpennemUserResponse

&#x20;|      Get current user information.

&#x20;|  

&#x20;|  get\_facilities(self, facility\_code: list\[str] | None = None, status\_id: list\[openelectricity.types.UnitStatusType | str] | None = None, fueltech\_id: list\[openelectricity.types.UnitFueltechType | str] | None = None, network\_id: list\[str] | None = None, network\_region: str | None = None) -> openelectricity.models.facilities.FacilityResponse

&#x20;|      Get a list of facilities.

&#x20;|  

&#x20;|  get\_facility\_data(self, network\_code: Literal\['NEM', 'WEM', 'AU'], facility\_code: str | list\[str] | None = None, metrics: list\[openelectricity.types.DataMetric] | None = None, interval: Optional\[Literal\['5m', '1h', '1d', '7d', '1M', '3M', 'season', '1y', 'fy']] = None, date\_start: datetime.datetime | None = None, date\_end: datetime.datetime | None = None, unit\_code: str | list\[str] | None = None) -> openelectricity.models.timeseries.TimeSeriesResponse

&#x20;|      Get facility data for specified metrics.

&#x20;|      

&#x20;|      Note:

&#x20;|          The API accepts at most 30 ``facility\_code`` (or ``unit\_code``)

&#x20;|          items per request. Larger lists raise

&#x20;|          :class:`OpenElectricityError` before the request is sent — split

&#x20;|          into chunks of 30 or filter further.

&#x20;|  

&#x20;|  get\_market(self, network\_code: Literal\['NEM', 'WEM', 'AU'], metrics: list\[openelectricity.types.MarketMetric], interval: Optional\[Literal\['5m', '1h', '1d', '7d', '1M', '3M', 'season', '1y', 'fy']] = None, date\_start: datetime.datetime | None = None, date\_end: datetime.datetime | None = None, primary\_grouping: Optional\[Literal\['network', 'network\_region']] = None, network\_region: str | None = None) -> openelectricity.models.timeseries.TimeSeriesResponse

&#x20;|      Get market data for specified metrics.

&#x20;|  

&#x20;|  get\_network\_data(self, network\_code: Literal\['NEM', 'WEM', 'AU'], metrics: list\[openelectricity.types.DataMetric], interval: Optional\[Literal\['5m', '1h', '1d', '7d', '1M', '3M', 'season', '1y', 'fy']] = None, date\_start: datetime.datetime | None = None, date\_end: datetime.datetime | None = None, network\_region: str | None = None, fueltech: list\[openelectricity.types.UnitFueltechType] | None = None, fueltech\_group: list\[openelectricity.types.FueltechGroupType] | None = None, primary\_grouping: Optional\[Literal\['network', 'network\_region']] = None, secondary\_grouping: Optional\[Literal\['fueltech', 'fueltech\_group', 'status', 'renewable']] = None) -> openelectricity.models.timeseries.TimeSeriesResponse

&#x20;|      Get network data for specified metrics.

&#x20;|      

&#x20;|      Args:

&#x20;|          network\_code: The network to get data for

&#x20;|          metrics: List of metrics to query (e.g. energy, power, price)

&#x20;|          interval: The time interval to aggregate by

&#x20;|          date\_start: Start time for the query

&#x20;|          date\_end: End time for the query

&#x20;|          network\_region: Network region to get data for

&#x20;|          fueltech: List of individual fuel technologies to filter by (UnitFueltechType enum values)

&#x20;|          fueltech\_group: List of fuel technology groups to filter by (FueltechGroupType enum values)

&#x20;|          primary\_grouping: Primary grouping to apply

&#x20;|          secondary\_grouping: Optional secondary grouping to apply

&#x20;|      

&#x20;|      Returns:

&#x20;|          TimeSeriesResponse: Time series data response containing a list of TimeSeries objects

&#x20;|  

&#x20;|  ----------------------------------------------------------------------

&#x20;|  Data descriptors inherited from BaseOEClient:

&#x20;|  

&#x20;|  \_\_dict\_\_

&#x20;|      dictionary for instance variables (if defined)

&#x20;|  

&#x20;|  \_\_weakref\_\_

&#x20;|      list of weak references to the object (if defined)



