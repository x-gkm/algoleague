use async_stream::try_stream;
use chrono::{DateTime, Utc};
use futures::Stream;
use reqwest::{IntoUrl, Method};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use url::Url;

static AUTH_URL: &str = "https://identity.algoleague.com/connect/token";
static API_BASE: &str = "https://api.algoleague.com/api/app";

#[derive(Debug, Serialize, Deserialize)]
struct JwtResponse {
    access_token: String,
    expires_in: i32,
    token_type: String,
    refresh_token: String,
    scope: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Pagination<T> {
    total_count: i32,
    items: Vec<T>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListResponse<T> {
    items: Vec<T>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndividualParticipant {
    profile: Profile,
    creation_time: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Profile {
    user_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TeamParticipant {
    team_name: String,
    creation_time: DateTime<Utc>,
}

pub struct Client {
    client: reqwest::Client,
    token: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Contest {
    pub id: String,
    pub name: String,
    pub slug: String,
    pub start_date: DateTime<Utc>,
    pub end_date: DateTime<Utc>,
    pub participation_type: ContestParticipationType,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum ContestParticipationType {
    Individual,
    Team,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Submission {
    pub problem_id: String,
    pub problem_slug: String,
    pub status: SubmissionStatus,
    pub during_contest: bool,
    pub user_name: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum SubmissionStatus {
    Accepted,
    CompileError,
    MemoryLimitExceeded,
    RuntimeError,
    TimeLimitExceeded,
    WrongAnswer,
    Processing,
    Pending,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Problem {
    pub id: String,
    pub slug: String,
}

#[derive(Debug)]
pub struct Participant {
    pub name: String,
    pub creation_time: DateTime<Utc>,
}

impl From<IndividualParticipant> for Participant {
    fn from(ip: IndividualParticipant) -> Self {
        Self {
            name: ip.profile.user_name,
            creation_time: ip.creation_time,
        }
    }
}

impl From<TeamParticipant> for Participant {
    fn from(tp: TeamParticipant) -> Self {
        Self {
            name: tp.team_name,
            creation_time: tp.creation_time,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("error parsing url: {0}")]
    Url(#[from] url::ParseError),
}

impl Client {
    pub async fn login(username: &str, password: String) -> Result<Self, Error> {
        let client = reqwest::Client::new();

        let form = [
            ("grant_type", "password"),
            (
                "scope",
                "offline_access openid profile role email phone Ojs",
            ),
            ("username", username),
            ("password", &password),
            ("client_id", "Ojs_App"),
        ];

        let jwt_response = client
            .request(Method::POST, AUTH_URL)
            .form(&form)
            .send()
            .await?
            .json::<JwtResponse>()
            .await?;

        Ok(Client {
            client,
            token: format!("Bearer {}", jwt_response.access_token),
        })
    }

    async fn get_json<T>(&self, url: impl IntoUrl) -> Result<T, Error>
    where
        T: DeserializeOwned,
    {
        Ok(self
            .client
            .request(Method::GET, url)
            .header("Authorization", &self.token)
            .send()
            .await?
            .json()
            .await?)
    }

    fn paginate<T>(&self, url: String) -> impl Stream<Item = Result<T, Error>>
    where
        T: DeserializeOwned,
    {
        let max_result_count = 10;
        let mut skip_count = 0;

        try_stream! {
            loop {
                let url = Url::parse_with_params(
                    &url,
                    [
                        ("SkipCount", skip_count.to_string()),
                        ("MaxResultCount", max_result_count.to_string()),
                    ],
                )?;


                let response: Pagination<T> = self.get_json(url).await?;

                if response.items.len() == 0 {
                    break;
                }

                for item in response.items {
                    yield item;
                }

                skip_count += max_result_count;
            }
        }
    }

    pub fn contests(&self) -> impl Stream<Item = Result<Contest, Error>> {
        self.paginate(format!(
            "{}/contests?Content=Contest&Filter=&ContestActivity=Archived&CombineWith=And",
            API_BASE
        ))
    }

    pub fn submissions(&self, contest_id: String) -> impl Stream<Item = Result<Submission, Error>> {
        try_stream! {
            let url = Url::parse_with_params(
                &format!("{}/problem-submission-results/all-by-contest?AllSubmissions=true", API_BASE),
                [("ContestId", contest_id)],
            )?
            .to_string();

            for await page in self.paginate(url) {
                yield page?;
            }
        }
    }

    pub fn participants(
        &self,
        contest_slug: &str,
    ) -> impl Stream<Item = Result<Participant, Error>> {
        try_stream! {
            let mut url = Url::parse(&format!("{}/contests/by-slug", API_BASE))?;
            url.path_segments_mut().unwrap().push(&contest_slug);

            let contest: Contest = self.get_json(url).await?;

            let kind = match contest.participation_type {
                ContestParticipationType::Individual => {
                    "user"
                }
                ContestParticipationType::Team => {
                    "team"
                }
            };

            let url = Url::parse_with_params(
                &format!("{}/contests-{}-sign-ups", API_BASE, kind),
                [("ContestId", &contest.id)],
            )?
            .to_string();

            match contest.participation_type {
                ContestParticipationType::Individual => {
                    for await page in self.paginate::<IndividualParticipant>(url) {
                        yield page?.into();
                    }
                }
                ContestParticipationType::Team => {
                    for await page in self.paginate::<TeamParticipant>(url) {
                        yield page?.into();
                    }
                }
            }
        }
    }

    pub async fn problems(&self, contest_id: &str) -> Result<Vec<Problem>, Error> {
        Ok(self
            .get_json::<ListResponse<_>>(Url::parse_with_params(
                &format!("{}/contests-problem/get-problems-info", API_BASE),
                [("ContestId", contest_id)],
            )?)
            .await?
            .items)
    }
}
