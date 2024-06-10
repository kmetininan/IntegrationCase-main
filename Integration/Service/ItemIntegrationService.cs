using Integration.Common;
using Integration.Backend;
using System.Collections.Concurrent;
using org.apache.zookeeper;

namespace Integration.Service;

public sealed class ItemIntegrationService
{
    //This is a dependency that is normally fulfilled externally.
    private ItemOperationBackend ItemIntegrationBackend { get; set; } = new();
    private ZooKeeper zooKeeper;
    private int identifier = 0;
    private ConcurrentDictionary<string, string> itemContents = new ConcurrentDictionary<string, string>();

    public ItemIntegrationService(ZooKeeper zooKeeper)
    {
        this.zooKeeper = zooKeeper;
    }

    // This is called externally and can be called multithreaded, in parallel.
    // More than one item with the same content should not be saved. However,
    // calling this with different contents at the same time is OK, and should
    // be allowed for performance reasons.

    public Result SaveItem(string itemContent)
    {
        // Attempt to acquire the lock
        var lockPath = zooKeeper.createAsync("/itemLock", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        if (lockPath == null)
        {
            return new Result(false, $"Failed to acquire lock. Item not saved.{itemContent}.");
        }

        try
        {
            // Check the backend to see if the content is already saved.
            if (ItemIntegrationBackend.FindItemsWithContent(itemContent).Count != 0)
            {
                return new Result(false, $"Duplicate item received with content {itemContent}.");
            }

            //check memory if the content is already saved.
            if (itemContents.Any(item => item.Value == itemContent))
            {
                return new Result(false, $"Duplicate item received with content {itemContent}.");
            }

            var item = ItemIntegrationBackend.SaveItem(itemContent);
            string itemId = (++identifier).ToString();
            itemContents.TryAdd(itemId, itemContent);

            return new Result(true, $"Item with content {itemContent} saved with id {item.Id}");
            
        }
        finally
        {
            zooKeeper.deleteAsync("/itemLock").Wait();
        }
    }

    public List<Item> GetAllItems()
    {
        return ItemIntegrationBackend.GetAllItems();
    }
}