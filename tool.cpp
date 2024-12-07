#include <iostream>
#include <kvstore.h>

using namespace std::chrono_literals;
using namespace KVSTORE_NS::literals;
using namespace KVSTORE_NS;

int main()
{
    kvstore store{kvstore::config_options{}};

    while (true)
    {
        std::cout << "Please enter command (quit | get <key> | put <key> <value>): ";
        std::string line{};
        std::getline(std::cin, line);

        std::string cmd{};
        std::string key{};
        std::string val{};
        std::stringstream ss{line};
        std::vector<std::byte> data{};
        if (std::getline(ss, cmd, ' '))
        {
            if (cmd == "quit") { break; }
            else if (cmd == "get" && std::getline(ss, key, ' '))
            {
                if (store.get(key, data))
                {
                    val=std::string{reinterpret_cast<char const *>(data.data()), data.size()};
                    std::cout << "GET " << key << ":" << val << std::endl;
                }
                else { std::cout << "GET " << key << ":" << "not found" << std::endl; }
            }
            else if (cmd == "put" && std::getline(ss, key, ' ') && std::getline(ss, val, ' '))
            {
                store.put(key, val.data(), val.size());
                std::cout << "PUT " << key << ":" << val << std::endl;
            }
        }
    }

    return 0;
}
