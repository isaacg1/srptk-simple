use rand::prelude::*;
use rand_distr::Exp;
use noisy_float::prelude::*;

use std::f64::INFINITY;
const EPSILON: f64 = 1e-8;

#[derive(Copy, Clone, Debug)]
enum Dist {
    Hyperexp(f64, f64, f64),
}

impl Dist {
    fn sample<R: Rng>(&self, rng: &mut R) -> f64 {
        match self {
            Dist::Hyperexp(low_mu, high_mu, prob_low) => {
                let mu = if rng.gen::<f64>() > *prob_low {
                    low_mu
                } else {
                    high_mu
                };
                Exp::new(*mu).unwrap().sample(rng)
            }
        }
    }
    fn mean(&self) -> f64 {
        use Dist::*;
        match self {
            Hyperexp(low_mu, high_mu, prob_low) => prob_low / low_mu + (1.0 - prob_low) / high_mu,
        }
    }
}

#[derive(Debug)]
struct Job {
    arrival_time: f64,
    rem_size: f64,
}

fn simulate(num_servers: usize, num_jobs: u64, dist: Dist, rho: f64, seed: u64) -> f64 {
    assert!((dist.mean() - 1.0).abs() < EPSILON);
    let mut queue: Vec<Job> = vec![];
    let mut num_completions = 0;
    let mut total_response = 0.0;
    let mut time = 0.0;
    let mut rng = StdRng::seed_from_u64(seed);
    let arrival_dist = Exp::new(rho).unwrap();
    let mut next_arrival_time = arrival_dist.sample(&mut rng);
    while num_completions < num_jobs {
        queue.sort_by_key(|job| n64(job.rem_size));
        let next_completion = queue
            .iter()
            .take(num_servers)
            .map(|job| job.rem_size * num_servers as f64)
            .min_by_key(|f| n64(*f))
            .unwrap_or(INFINITY);
        let next_duration = next_completion.min(next_arrival_time - time);
        let was_arrival = next_duration < next_completion;
        time += next_duration;
        queue
            .iter_mut()
            .take(num_servers)
            .for_each(|job| job.rem_size -= next_duration / num_servers as f64);
        for i in (0..num_servers.min(queue.len())).rev() {
            if queue[i].rem_size < EPSILON {
                let job = queue.remove(i);
                total_response += time - job.arrival_time;
                num_completions += 1;
            }
        }
        if was_arrival {
            let new_size = dist.sample(&mut rng);
            let new_job = Job {
                rem_size: new_size,
                arrival_time: time,
            };
            queue.push(new_job);
            next_arrival_time = time + arrival_dist.sample(&mut rng);
        }
    }
    total_response / num_completions as f64
}

fn main() {
    //let dist = Dist::Hyperexp(2.0, 2.0 / 3.0, 0.5);
    let dist = Dist::Hyperexp(1.0, 1.0, 1.0);
    let rhos = vec![
        0.01, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.72,
        0.74, 0.76, 0.78, 0.8, 0.82, 0.84, 0.86, 0.88, 0.9, 0.903, 0.906, 0.91, 0.913, 0.916, 0.92,
        0.923, 0.926, 0.93, 0.933, 0.936, 0.94, 0.943, 0.946, 0.95, 0.953, 0.956, 0.96, 0.97,
        0.973, 0.976, 0.98, 0.983, 0.986, 0.99, 0.993, 0.996,
    ];
    let seed = 0;
    let num_jobs = 10_000_000;
    let num_servers = 2;
    println!(
        "num_jobs {} num_servers {} seed {} dist {:?}",
        num_jobs, num_servers, seed, dist
    );
    for rho in rhos {
        let response = simulate(num_servers, num_jobs, dist, rho, seed);
        println!("{};{};", rho, response);
    }
}
