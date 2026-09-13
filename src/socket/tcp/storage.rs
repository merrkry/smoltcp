//! Storage and host services needed by a single TCP connection.
use crate::{storage::RingBuffer, time::Instant, wire::IpAddress};

/// Logical byte storage. RX may write beyond `len()` for out-of-order data;
/// those bytes must survive dequeues. TX must retain bytes until acknowledged.
/// All slices are borrowed only for the duration of the calling operation.
pub trait Buffer {
    fn capacity(&self) -> usize;
    fn len(&self) -> usize;
    fn clear(&mut self);
    fn window(&self) -> usize {
        self.capacity() - self.len()
    }
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn is_full(&self) -> bool {
        self.window() == 0
    }
    /// Override the handshake window scale independently of initial storage.
    fn window_shift(&self) -> Option<u8> {
        None
    }
    fn enqueue_slice(&mut self, data: &[u8]) -> usize;
    fn dequeue_slice(&mut self, data: &mut [u8]) -> usize;
    fn get_allocated(&self, offset: usize, size: usize) -> &[u8];
    fn read_allocated(&mut self, offset: usize, data: &mut [u8]) -> usize;
    fn write_unallocated(&mut self, offset: usize, data: &[u8]) -> usize;
    fn enqueue_unallocated(&mut self, count: usize);
    fn dequeue_allocated(&mut self, count: usize);
}

impl Buffer for RingBuffer<'_, u8> {
    fn capacity(&self) -> usize {
        self.capacity()
    }
    fn len(&self) -> usize {
        self.len()
    }
    fn clear(&mut self) {
        self.clear()
    }
    fn enqueue_slice(&mut self, data: &[u8]) -> usize {
        self.enqueue_slice(data)
    }
    fn dequeue_slice(&mut self, data: &mut [u8]) -> usize {
        self.dequeue_slice(data)
    }
    fn get_allocated(&self, offset: usize, size: usize) -> &[u8] {
        self.get_allocated(offset, size)
    }
    fn read_allocated(&mut self, offset: usize, data: &mut [u8]) -> usize {
        self.read_allocated(offset, data)
    }
    fn write_unallocated(&mut self, offset: usize, data: &[u8]) -> usize {
        self.write_unallocated(offset, data)
    }
    fn enqueue_unallocated(&mut self, count: usize) {
        self.enqueue_unallocated(count)
    }
    fn dequeue_allocated(&mut self, count: usize) {
        self.dequeue_allocated(count)
    }
}

/// Host services for direct TCP driving. The caller validates IP/TCP headers,
/// checksums and flow ownership before `process`, and owns packet emission.
pub trait TcpContext {
    fn now(&self) -> Instant;
    fn random_u32(&mut self) -> u32;
    fn ip_mtu(&self) -> usize;
    fn max_transmission_unit(&self) -> usize {
        self.ip_mtu()
    }
    #[cfg(feature = "segmentation-offload")]
    fn segmentation_caps(&self) -> crate::phy::SegmentationCapabilities {
        crate::phy::SegmentationCapabilities::default()
    }
    fn has_ip_addr(&self, address: IpAddress) -> bool;
    fn get_source_address(&self, destination: &IpAddress) -> Option<IpAddress>;
}

impl TcpContext for crate::iface::Context {
    fn now(&self) -> Instant {
        self.now()
    }
    fn random_u32(&mut self) -> u32 {
        self.rand().rand_u32()
    }
    fn ip_mtu(&self) -> usize {
        self.ip_mtu()
    }
    fn max_transmission_unit(&self) -> usize {
        self.max_transmission_unit()
    }
    #[cfg(feature = "segmentation-offload")]
    fn segmentation_caps(&self) -> crate::phy::SegmentationCapabilities {
        self.segmentation_caps()
    }
    fn has_ip_addr(&self, address: IpAddress) -> bool {
        self.has_ip_addr(address)
    }
    fn get_source_address(&self, destination: &IpAddress) -> Option<IpAddress> {
        self.get_source_address(destination)
    }
}

/// Optional contiguous mutable access for the conventional socket API.
pub trait ContiguousBuffer: Buffer {
    fn enqueue_many_with<'b, R, F: FnOnce(&'b mut [u8]) -> (usize, R)>(
        &'b mut self,
        f: F,
    ) -> (usize, R);
    fn dequeue_many_with<'b, R, F: FnOnce(&'b mut [u8]) -> (usize, R)>(
        &'b mut self,
        f: F,
    ) -> (usize, R);
}

impl ContiguousBuffer for RingBuffer<'_, u8> {
    fn enqueue_many_with<'b, R, F: FnOnce(&'b mut [u8]) -> (usize, R)>(
        &'b mut self,
        f: F,
    ) -> (usize, R) {
        self.enqueue_many_with(f)
    }
    fn dequeue_many_with<'b, R, F: FnOnce(&'b mut [u8]) -> (usize, R)>(
        &'b mut self,
        f: F,
    ) -> (usize, R) {
        self.dequeue_many_with(f)
    }
}
