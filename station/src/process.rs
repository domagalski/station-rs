use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub enum ProcessAction {
    StartProcess(String),
    StopProcess(String),
}

pub struct Process {
    name: String,
}

impl Process {
    pub fn new(name: &str) -> Process {
        Process {
            name: String::from(name),
        }
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_process() {
        let process = Process::new("test-process");
        assert_eq!(process.name(), "test-process");
    }
}
