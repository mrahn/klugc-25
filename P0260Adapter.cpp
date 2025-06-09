#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>

struct Task
{
  int i;
  int m;
};

template<class Queue>
  struct P0260TaskQueueAdaptor
{
  struct Client
  {
    auto push (Task task) const -> void
    {
      auto increased_number_of_enqueued_tasks
        { ScopedIncrement {_task_queue->_number_of_enqueued_tasks}
        };

      if (!_task_queue->_queue.push (std::move (task)))
      {
        throw std::logic_error {"push on closed TaskQueue"};
      }

      increased_number_of_enqueued_tasks.release();
    }

    auto pop() const -> std::optional<Task>
    {
      if ( auto task
           { std::invoke
             ( [&]() -> std::optional<Task>
               {
                 auto const increased_number_of_running_pop
                   { ScopedIncrement {_task_queue->_number_of_running_pop}
                   };

                 _task_queue->_close_if_quiescent();

                 return _task_queue->_queue.pop();
               }
             )
           }
        )
      {
        _task_queue->_number_of_enqueued_tasks.fetch_sub (1);

        return task;
      }

      return {};
    }

    explicit Client (P0260TaskQueueAdaptor* task_queue)
      : _task_queue {task_queue}
    {}

    struct Deleter
    {
      auto operator() (Client* client) const -> void
      {
        client->_task_queue->_number_of_clients.fetch_sub (1);
        client->_task_queue->_close_if_quiescent();

        return std::default_delete<Client>{} (client);
      }
    };

  private:
    P0260TaskQueueAdaptor* _task_queue;
  };

  template<class... Args>
    explicit P0260TaskQueueAdaptor (Args&&... args)
      : _queue {std::forward<Args> (args)...}
  {}

  auto client() -> std::unique_ptr<Client, typename Client::Deleter>
  {
    auto client
      { std::unique_ptr<Client, typename Client::Deleter>
          {new Client {this}, {}}
      };

    _number_of_clients.fetch_add (1);
    return client;
  }

private:
  struct ScopedIncrement
  {
    explicit ScopedIncrement (std::atomic<std::size_t>& value)
      : _value {value}
    {
      _value.fetch_add (1);
    }

    ScopedIncrement (ScopedIncrement const&) = delete;
    ScopedIncrement (ScopedIncrement&&) = delete;
    auto operator= (ScopedIncrement const&) -> ScopedIncrement& = delete;
    auto operator= (ScopedIncrement&&) -> ScopedIncrement& = delete;

    auto release() -> void
    {
      _active = false;
    }

    ~ScopedIncrement()
    {
      if (_active)
      {
        _value.fetch_sub (1);
      }
    }

  private:
    std::atomic<std::size_t>& _value;
    bool _active {true};
  };

  auto _close_if_quiescent() -> void
  {
    if (  !_queue.is_closed()
       && _number_of_enqueued_tasks.load() == 0
       && _number_of_running_pop.load() == _number_of_clients.load())
    {
      _queue.close();
    }
  }

  Queue _queue;
  std::atomic<std::size_t> _number_of_clients {0};
  std::atomic<std::size_t> _number_of_running_pop {0};
  std::atomic<std::size_t> _number_of_enqueued_tasks {0};
};
