// ${CXX} --std=c++20 -Wall tq.cpp

#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <future>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <random>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

struct Task
{
  int i;
  int m;
};

namespace
{
  auto random_device() -> std::default_random_engine&
  {
    static auto const seed ( std::chrono::high_resolution_clock::now()
                           . time_since_epoch().count()
                           );
    using Engine = std::default_random_engine;
    static auto d {Engine {static_cast<Engine::result_type> (seed)}};
    return d;
  }
}

auto process (int t, Task task) -> std::vector<Task>
{
  static auto delay_time {std::uniform_int_distribution<int> {0, 100}};
  auto const duration {delay_time (random_device())};

  {
    static auto print_guard {std::mutex{}};
    auto const lock {std::lock_guard {print_guard}};
    std::cout << "process " << t
              << " task " << task.i
              << " duration " << duration << " ms"
              << '\n'
      ;
  }

  std::this_thread::sleep_for (std::chrono::milliseconds {duration});

  if (task.i < task.m)
  {
    return std::vector<Task>
      { Task {2 * task.i + 1, task.m}
      , Task {2 * task.i + 2, task.m}
      };
  }

  return std::vector<Task>{};
}

struct TaskQueue
{
  struct Client
  {
    auto push (Task) const -> void;
    auto pop() const -> std::optional<Task>;

    Client (TaskQueue*);

    struct Deleter
    {
      auto operator() (Client*) const -> void;
    };

  private:
    TaskQueue* _task_queue;
  };
  auto client() -> std::unique_ptr<Client, Client::Deleter>;

private:
  auto _close_if_quiescent (std::unique_lock<std::mutex> const&) -> void;

  std::size_t _number_of_clients {0};
  std::size_t _number_of_running_pop {0};
  bool _closed {false};
  std::mutex _guard;
  std::condition_variable _task_added_or_number_of_running_pop_changed;
  std::deque<Task> _queue;
};

TaskQueue::Client::Client (TaskQueue* task_queue)
  : _task_queue {task_queue}
{}

auto TaskQueue::Client::Deleter::operator() (Client* client) const -> void
{
  auto lock {std::unique_lock {client->_task_queue->_guard}};
  --client->_task_queue->_number_of_clients;
  client->_task_queue->_close_if_quiescent (lock);
  client->_task_queue->_task_added_or_number_of_running_pop_changed
    .notify_all()
    ;
  return std::default_delete<Client>{} (client);
}

auto TaskQueue::client() -> std::unique_ptr<Client, Client::Deleter>
{
  auto const lock {std::lock_guard {_guard}};
  ++_number_of_clients;
  return std::unique_ptr<Client, Client::Deleter> {new Client {this}, {}};
}

auto TaskQueue::_close_if_quiescent
  ( std::unique_lock<std::mutex> const&
#ifndef NDEBUG
    lock
#endif
  ) -> void
{
  assert (lock.mutex() == std::addressof (_guard));
  assert (lock.owns_lock());

  if (  !_closed
     && _queue.empty()
     && _number_of_running_pop == _number_of_clients
     )
  {
    _closed = true;
    _task_added_or_number_of_running_pop_changed.notify_all();
  }
}

auto TaskQueue::Client::push (Task task) const -> void
{
  auto const lock {std::lock_guard {_task_queue->_guard}};

  if (_task_queue->_closed)
  {
    throw std::logic_error {"push on closed TaskQueue"};
  }

  _task_queue->_queue.emplace_back (task);
  _task_queue->_task_added_or_number_of_running_pop_changed.notify_one();
}

auto TaskQueue::Client::pop() const -> std::optional<Task>
{
  struct ScopedIncrement
  {
    ScopedIncrement (std::size_t& value) : _value {value} { ++_value; }
    ScopedIncrement (ScopedIncrement const&) = delete;
    ScopedIncrement (ScopedIncrement&&) = delete;
    auto operator= (ScopedIncrement const&) -> ScopedIncrement& = delete;
    auto operator= (ScopedIncrement&&) -> ScopedIncrement& = delete;
    ~ScopedIncrement() { --_value; }
  private:
    std::size_t& _value;
  };

  auto lock {std::unique_lock {_task_queue->_guard}};

  auto const scoped_increment
    { ScopedIncrement {_task_queue->_number_of_running_pop}
    };

  _task_queue->_close_if_quiescent (lock);

  _task_queue->_task_added_or_number_of_running_pop_changed.wait
    ( lock
    , [&]
      {
        return !_task_queue->_queue.empty() || _task_queue->_closed;
      }
    );

  if (!_task_queue->_queue.empty())
  {
    auto x {_task_queue->_queue.front()};
    _task_queue->_queue.pop_front();
    return x;
  }

  return {};
}

int main (int argc, char** argv)
try
{
  if (argc != 3)
  {
    throw std::invalid_argument {"usage: tq max_i #prosumer"};
  }

  auto const max_i {std::stoi (argv[1])};
  auto const number_of_prosumer {std::stoi (argv[2])};

  using Clock = std::chrono::steady_clock;

  auto const start {Clock::now()};

  auto queue {TaskQueue{}};

  queue.client()->push (Task {0, max_i});

  auto prosumers {std::vector<std::future<std::size_t>>{}};

  auto print_guard {std::mutex{}};

  for (auto i {0}; i != number_of_prosumer; ++i)
  {
    prosumers.emplace_back
      ( std::async
        ( std::launch::async
        , [&queue, &print_guard, t = i]
          {
            auto number_of_tasks {std::size_t {0}};
            auto const client {queue.client()};

            while (auto const task {client->pop()})
            {
              ++number_of_tasks;
              for (auto child : process (t, *task))
              {
                client->push (child);
              }
            };

            {
              auto const lock {std::lock_guard {print_guard}};
              std::cout << "process " << t << " done"
                        << " (#tasks " << number_of_tasks << ")"
                        << '\n';
            }

            return number_of_tasks;
          }
        )
      );
  }

  auto const number_of_tasks
    { std::accumulate
      ( std::begin (prosumers), std::end (prosumers)
      , std::size_t {0}
      , [] (auto sum, auto& prosumer)
        {
          return sum + prosumer.get();
        }
      )
    };

  auto const end {Clock::now()};
  auto const execution_time
    {std::chrono::duration_cast<std::chrono::milliseconds> (end - start)};

  std::cout << "sum #tasks: " << number_of_tasks
            << " execution time " << execution_time.count() << " ms"
            << '\n';

  if (std::cmp_not_equal (number_of_tasks, 2 * max_i + 1))
  {
    throw std::runtime_error
      { "Unexpected number of finished tasks: "
      + std::to_string (number_of_tasks)
      + " != "
      + std::to_string (2 * max_i + 1)
      };
  }

  return EXIT_SUCCESS;
}
catch (std::exception const& e)
{
  std::cout << "Error: " << e.what() << '\n';

  return EXIT_FAILURE;
}
catch (...)
{
  std::cout << "Error: UNKNOWN\n";

  return EXIT_FAILURE;
}
