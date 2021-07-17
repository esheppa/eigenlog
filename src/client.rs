use super::*;
use reqwest::header;
use bincode_crate as bincode;

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
    
    pub async fn info(
        &self,
        client: &reqwest::Client,
    ) -> Result<Vec<TreeName>> {
        todo!()
    }
}

