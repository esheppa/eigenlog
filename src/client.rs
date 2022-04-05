use super::*;
#[cfg(feature = "bincode")]
use bincode_crate as bincode;
#[allow(unused_imports)]
#[cfg(not(feature = "wasm"))]
use std::time;

impl<T> ApiConfig<T>
where
    T: ConnectionProxy,
{
    #[cfg(all(feature = "json", not(feature = "bincode")))]
    pub async fn query(
        &self,
        client: &reqwest::Client,
        params: &QueryParams,
        #[cfg(not(feature = "wasm"))] timeout: time::Duration,
    ) -> Result<Vec<QueryResponse>> {
        let url = self.base_url.join("query")?;

        let req = client.get(url);

        let req = self.proxy.clone().proxy(req).await?;

        let query = req
            .header(
                header::ACCEPT,
                header::HeaderValue::from_static(APPLICATION_JSON),
            )
            .query(&params);

        #[cfg(not(feature = "wasm"))]
        let query = query.timeout(timeout);

        let resp = query.send().await?.error_for_status()?.json().await?;

        Ok(resp)
    }

    #[cfg(feature = "bincode")]
    pub async fn query(
        &self,
        client: &reqwest::Client,
        params: &QueryParams,
        #[cfg(not(feature = "wasm"))] timeout: time::Duration,
    ) -> Result<Vec<QueryResponse>> {
        let url = self.base_url.join("query")?;

        let req = client.get(url);

        let req = self.proxy.clone().proxy(req).await?;

        let query = req
            .header(
                header::ACCEPT,
                header::HeaderValue::from_static(OCTET_STREAM),
            )
            .query(&params);

        #[cfg(not(feature = "wasm"))]
        let query = query.timeout(timeout);

        let resp = query.send().await?.error_for_status()?.bytes().await?;
        Ok(bincode::deserialize(&resp)?)
    }

    #[cfg(all(feature = "json", not(feature = "bincode")))]
    pub async fn detail(
        &self,
        client: &reqwest::Client,
        host: &Host,
        app: &App,
        level: Level,
    ) -> Result<LogTreeDetail> {
        let url = self
            .base_url
            .join(&format!("detail/{host}/{app}/{level}"))?;

        let req = client.get(url);

        let req = self.proxy.clone().proxy(req).await?;

        let resp = req
            .header(
                header::ACCEPT,
                header::HeaderValue::from_static(APPLICATION_JSON),
            )
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(resp)
    }

    #[cfg(feature = "bincode")]
    pub async fn detail(
        &self,
        client: &reqwest::Client,
        host: &Host,
        app: &App,
        level: Level,
    ) -> Result<LogTreeDetail> {
        let url = self
            .base_url
            .join(&format!("detail/{host}/{app}/{level}"))?;

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

    #[cfg(all(feature = "json", not(feature = "bincode")))]
    pub async fn info(
        &self,
        client: &reqwest::Client,
    ) -> Result<Vec<result::Result<LogTreeInfo, ParseLogTreeInfoError>>> {
        let url = self.base_url.join("info")?;

        let req = client.get(url);

        let req = self.proxy.clone().proxy(req).await?;

        let resp = req
            .header(
                header::ACCEPT,
                header::HeaderValue::from_static(APPLICATION_JSON),
            )
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(resp)
    }

    #[cfg(feature = "bincode")]
    pub async fn info(
        &self,
        client: &reqwest::Client,
    ) -> Result<Vec<result::Result<LogTreeInfo, ParseLogTreeInfoError>>> {
        let url = self.base_url.join("info")?;

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
