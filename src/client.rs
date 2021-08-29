use super::*;
use bincode_crate as bincode;
use reqwest::header;

impl ApiConfig {
    pub async fn query(
        &self,
        client: &reqwest::Client,
        params: &QueryParams,
    ) -> Result<Vec<QueryResponse>> {
        let url = format!("{}/query", self.base_url);

        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::HeaderName::from_static(API_KEY_HEADER),
            header::HeaderValue::from_str(&self.api_key)?,
        );

        headers.insert(
            header::ACCEPT,
            header::HeaderValue::from_static(OCTET_STREAM),
        );

        let resp = client
            .get(url)
            .headers(headers)
            .query(&params)
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

        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::HeaderName::from_static(API_KEY_HEADER),
            header::HeaderValue::from_str(&self.api_key)?,
        );

        headers.insert(
            header::ACCEPT,
            header::HeaderValue::from_static(OCTET_STREAM),
        );

        let resp = client
            .get(url)
            .headers(headers)
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

        let mut headers = header::HeaderMap::new();

        headers.insert(
            header::HeaderName::from_static(API_KEY_HEADER),
            header::HeaderValue::from_str(&self.api_key)?,
        );

        headers.insert(
            header::ACCEPT,
            header::HeaderValue::from_static(OCTET_STREAM),
        );

        let resp = client
            .get(url)
            .headers(headers)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        Ok(bincode::deserialize(&resp)?)
    }
}
