use super::*;
use bincode_crate as bincode;
use reqwest::header;
use std::time;

impl<T> ApiConfig<T>
where
    T: ConnectionProxy,
{
    pub async fn query(
        &self,
        client: &reqwest::Client,
        params: &QueryParams,
        timeout: time::Duration,
    ) -> Result<Vec<QueryResponse>> {
        let url = format!("{}/query", self.base_url);

        let req = client.get(url);

        let req = self.proxy.clone().proxy(req).await?;

        let resp = req
            .header(
                header::ACCEPT,
                header::HeaderValue::from_static(OCTET_STREAM),
            )
            .query(&params)
            .timeout(timeout)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        Ok(bincode::deserialize(&resp)?)
    }

    pub async fn detail(
        &self,
        client: &reqwest::Client,
        host: &Host,
        app: &App,
        level: Level,
    ) -> Result<LogTreeDetail> {
        let url = format!("{}/detail/{}/{}/{}", self.base_url, host, app, level);

        let req = client.get(url);

        let req = self.proxy.clone().proxy(req).await?;

        let resp = req
            .header(
                header::ACCEPT,
                header::HeaderValue::from_static(OCTET_STREAM),
            )
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        Ok(bincode::deserialize(&resp)?)
    }

    pub async fn info(
        &self,
        client: &reqwest::Client,
    ) -> Result<Vec<result::Result<LogTreeInfo, db::ParseLogTreeInfoError>>> {
        let url = format!("{}/info", self.base_url);

        let req = client.get(url);

        let req = self.proxy.clone().proxy(req).await?;

        let resp = req
            .header(
                header::ACCEPT,
                header::HeaderValue::from_static(OCTET_STREAM),
            )
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        Ok(bincode::deserialize(&resp)?)
    }
}
