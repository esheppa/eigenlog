pub fn __check_command(mut cmd: std::process::Command) -> Result<(), String> {
    let status = cmd
        .stderr(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .status()
        .map_err(|e| e.to_string())?;

    match status.code() {
        Some(code) if code > 0 => Err(format!("Failed with code: {code}")),
        _ => Ok(()),
    }
}

pub fn __get_output(mut cmd: std::process::Command) -> Result<String, String> {
    let output = cmd
        .stderr(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .output()
        .map_err(|e| e.to_string())?;

    let out = String::from_utf8(output.stdout).map_err(|e| e.to_string())?;
    let err = String::from_utf8_lossy(&output.stderr);
    if output.stderr.is_empty() {
        Ok(out)
    } else {
        Err(err.to_string())
    }
}

#[macro_export]
macro_rules! cmd {
    ( $cmd:literal $(,)? ) => {
        std::process::Command::new($cmd)
    };
    ( $cmd:literal, $( $arg:expr ),+ $(,)? ) => {{
        let mut cmd = std::process::Command::new($cmd);
        cmd.args([
                $({
                    let a: &std::ffi::OsStr = $arg.as_ref();
                    a
                },)+
            ]);
        cmd

    }};
}

#[macro_export]
macro_rules! check {
    ( $cmd:literal $(,)? ) => {
        __check_command(cmd!($cmd))
    };
    ( $cmd:literal, $( $arg:expr ),+ $(,)? ) => {
        __check_command(cmd!($cmd, $( $arg ),+ ))
    };
}

#[macro_export]
macro_rules! output {
    ( $cmd:literal $(,)? ) => {
        __get_output(cmd!($cmd))
    };
    ( $cmd:literal, $( $arg:expr ),+ $(,)? ) => {
        __get_output(cmd!($cmd, $( $arg ),+ ))
    };
}
